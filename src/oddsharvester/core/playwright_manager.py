import logging
import os
from pathlib import Path
import random

from playwright.async_api import Page, Route, async_playwright

from oddsharvester.core.exceptions import AllProxiesExhaustedError
from oddsharvester.utils.constants import PLAYWRIGHT_BROWSER_ARGS, PLAYWRIGHT_BROWSER_ARGS_DOCKER
from oddsharvester.utils.utils import is_running_in_docker

# Third-party domains to block (synced from SofaScore backfill)
BLOCKED_THIRD_PARTY_DOMAINS = (
    "smartadserver.com",
    "googleadservices.com",
    "googlesyndication.com",
    "doubleclick.net",
    "googletagmanager.com",
    "google-analytics.com",
)

BLOCKED_RESOURCE_TYPES = {"image", "stylesheet", "font", "media"}

# Comprehensive anti-detection script to hide automation signatures
HAR_REPLAY_ENV_VAR = "ODDSHARVESTER_HAR_REPLAY"
HAR_RECORD_ENV_VAR = "ODDSHARVESTER_HAR_RECORD"
HAR_REPLAY_URL_PATTERN = "**oddsportal.com/**"

# Anti-detection script to hide automation signatures
STEALTH_SCRIPT = """
(function() {
    // Remove webdriver property entirely
    delete navigator.webdriver;
    Object.defineProperty(navigator, 'webdriver', {get: () => false});
    
    // Chrome runtime object
    window.chrome = { runtime: {}, App: {}, csi: function(){}, loadTimes: function(){} };
    
    // Mock plugins to look real
    const mockPlugins = [
        { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
        { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
        { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
    ];
    Object.defineProperty(navigator, 'plugins', {get: () => mockPlugins, enumerable: true});
    
    // Mock languages
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'zh-Hant', 'zh'], enumerable: true});
    
    // Remove automation-related permissions
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' || 
        parameters.name === 'midi' || 
        parameters.name === 'camera' ||
        parameters.name === 'microphone'
        ) ? Promise.resolve({ state: Notification.permission }) : originalQuery(parameters);
    
    // Patch getComputedStyle to hide automation
    const originalGetComputedStyle = window.getComputedStyle;
    window.getComputedStyle = (element, pseudoElement) => {
        const style = originalGetComputedStyle(element, pseudoElement);
        if (style.zoom !== undefined) style.zoom = '1';
        return style;
    };
    
    // Mock Connection information
    Object.defineProperty(navigator, 'connection', {get: () => ({
        effectiveType: '4g',
        downlink: 10,
        rtt: 50,
        downlinkMax: 1000
    }), enumerable: true});
    
    // Remove Playwright-specific variables
    delete window.__playwright_evaluator__;
    delete window.__playwright_unpatched__;
    
    // Canvas fingerprinting protection
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    
    CanvasRenderingContext2D.prototype.getImageData = function(sx, sy, sw, sh) {
        const imageData = originalGetImageData.call(this, sx, sy, sw, sh);
        // Add small random noise to pixels to simulate human behavior
        const data = imageData.data;
        for (let i = 0; i < data.length; i += 4) {
            data[i] = Math.min(255, data[i] + Math.floor(Math.random() * 3 - 1));
            data[i+1] = Math.min(255, data[i+1] + Math.floor(Math.random() * 3 - 1));
            data[i+2] = Math.min(255, data[i+2] + Math.floor(Math.random() * 3 - 1));
        }
        return imageData;
    };
    
    // WebGL fingerprinting protection
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        // Return fake values for renderer and vendor
        if (param === 37445) return 'Intel Inc.';  // UNMASKED_VENDOR_WEBGL
        if (param === 37446) return 'Intel Iris OpenGL Engine';  // UNMASKED_RENDERER_WEBGL
        return getParameter.call(this, param);
    };
    
    const getExtension = WebGLRenderingContext.prototype.getExtension;
    WebGLRenderingContext.prototype.getExtension = function(name) {
        if (name === 'WEBGL_debug_renderer_info') return null;  // Block this extension
        return getExtension.call(this, name);
    };
    
    // AudioContext fingerprinting protection
    const originalCreateDynamicsCompressor = AudioContext.prototype.createDynamicsCompressor;
    AudioContext.prototype.createDynamicsCompressor = function() {
        try {
            const compressor = originalCreateDynamicsCompressor.call(this);
            // Add small random noise to prevent fingerprinting
            const originalGetValueAtTime = compressor.threshold.getValueAtTime;
            compressor.threshold.getValueAtTime = function(value, time) {
                return originalGetValueAtTime.call(this, value + (Math.random() * 0.1 - 0.05), time);
            };
            return compressor;
        } catch(e) {
            return originalCreateDynamicsCompressor.call(this);
        }
    };
    
    // Patch OneTrust if it exists
    if (window.Optanon) {
        window.Optanon.IsAlertBoxClosed = () => true;
        window.Optanon.GetDomain = () => '';
    }
    
    // Set consent cookie immediately
    try {
        const consentValue = 'groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0005%3A1%2CC0006%3A1%2CC0007%3A1%2CC0008%3A1%2CC0009%3A1%2CC0010%3A1%2CC0011%3A1%2CC0012%3A1%2CC0013%3A1%2CC0014%3A1%2CC0015%3A1%2CC0016%3A1%2CC0017%3A1%2CC0018%3A1%2CC0019%3A1%2CC0020%3A1%2CC0021%3A1%2CC0022%3A1%2CC0023%3A1%2CC0024%3A1%2CC0025%3A1';
        document.cookie = 'OptanonConsent=' + consentValue + '; domain=.oddsportal.com; path=/; max-age=31536000';
        document.cookie = 'OptanonConsent=' + consentValue + '; domain=www.oddsportal.com; path=/; max-age=31536000';
        document.cookie = 'OptanonAlertBoxClosed=Sun%20Apr%2019%202026%2000%3A00%3A00%20GMT%2B0000%20(Coordinated%20Universal%20Time); domain=.oddsportal.com; path=/; max-age=31536000';
    } catch(e) {}
    
    // Override document.cookie to always return our consent cookies
    const originalCookieDescriptor = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie');
    const consentCookies = [
        'OptanonConsent=groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0005%3A1%2CC0006%3A1%2CC0007%3A1%2CC0008%3A1%2CC0009%3A1%2CC0010%3A1%2CC0011%3A1%2CC0012%3A1%2CC0013%3A1%2CC0014%3A1%2CC0015%3A1%2CC0016%3A1%2CC0017%3A1%2CC0018%3A1%2CC0019%3A1%2CC0020%3A1%2CC0021%3A1%2CC0022%3A1%2CC0023%3A1%2CC0024%3A1%2CC0025%3A1',
        'OptanonAlertBoxClosed=Sun%20Apr%2019%202026%2000%3A00%3A00%20GMT%2B0000%20(Coordinated%20Universal%20Time)'
    ];
    
    Object.defineProperty(Document.prototype, 'cookie', {
        get: function() {
            const cookies = originalCookieDescriptor.get.call(this);
            if (this.domain && (this.domain.includes('oddsportal') || this.domain === '')) {
                if (!cookies.includes('OptanonConsent')) {
                    return cookies + (cookies ? '; ' : '') + consentCookies.join('; ');
                }
            }
            return cookies;
        },
        set: function(value) {
            // Allow setting consent cookies, block others
            if (value && (value.includes('OptanonConsent') || value.includes('OptanonAlertBoxClosed'))) {
                return originalCookieDescriptor.set.call(this, value);
            }
            return originalCookieDescriptor.set.call(this, value);
        },
        configurable: true
    });
})();
"""

# Default user agents that look like real browsers (synced from SofaScore backfill)
DEFAULT_USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    # Chrome on Mobile (Android)
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    # Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


class PlaywrightManager:
    """
    Manages Playwright browser lifecycle and configuration.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.timezone_id: str | None = None
        self.contexts: dict = {}
        self._default_key: str | None = None
        self._proxy_manager = None

    async def initialize(
        self,
        headless: bool,
        user_agent: str | None = None,
        locale: str | None = None,
        timezone_id: str | None = None,
        proxy_manager=None,
    ):
        """
        Initialize and start Playwright with a browser and page.

        Args:
            is_webdriver_headless (bool): Whether to start the browser in headless mode.
            proxy_manager: Optional ProxyManager providing the launch proxy and, in multi-proxy
                mode, one context per proxy.
        """
        try:
            self.logger.info("Starting Playwright...")
            self.timezone_id = timezone_id
            self._proxy_manager = proxy_manager
            self.playwright = await async_playwright().start()

            browser_args = PLAYWRIGHT_BROWSER_ARGS_DOCKER if is_running_in_docker() else PLAYWRIGHT_BROWSER_ARGS
            launch_proxy = proxy_manager.launch_proxy() if proxy_manager else None
            self.browser = await self.playwright.chromium.launch(
                headless=headless, args=browser_args, proxy=launch_proxy
            )

            effective_user_agent = user_agent or random.choice(DEFAULT_USER_AGENTS)  # noqa: S311

            # (key, per-context proxy override) for each context to create.
            # Per-context proxy is used ONLY in multi-proxy mode; otherwise the
            # single context inherits the launch proxy (unchanged behavior).
            if proxy_manager and proxy_manager.is_multi_proxy():
                context_specs = [(e.key, e.config) for e in proxy_manager.entries]
            elif proxy_manager:
                context_specs = [(proxy_manager.entries[0].key, None)]
            else:
                context_specs = [("direct", None)]

            self._default_key = context_specs[0][0]
            for index, (key, ctx_proxy) in enumerate(context_specs):
                self.contexts[key] = await self._create_context(
                    proxy=ctx_proxy,
                    user_agent=effective_user_agent,
                    locale=locale,
                    timezone_id=timezone_id,
                    enable_har=(index == 0),
                )

            self.context = self.contexts[self._default_key]
            self.page = await self.context.new_page()

            # Block OneTrust scripts to prevent bot detection
            await self._block_one_trust_scripts(self.page)

            # When no explicit timezone is requested, the browser context falls
            # back to the host system timezone. Capture the effective timezone
            # so date-header parsing and DOM match-date conversion use the same
            # zone the browser actually rendered in (see docs/agentic-gotchas.md
            # §10).
            if self.timezone_id is None:
                try:
                    self.timezone_id = await self.page.evaluate(
                        "() => Intl.DateTimeFormat().resolvedOptions().timeZone"
                    )
                except Exception as e:
                    self.logger.warning(f"Could not resolve browser timezone, assuming UTC: {e}")
                    self.timezone_id = "UTC"

            self.logger.info("Playwright initialized successfully.")

        except Exception as e:
            self.logger.error(f"Failed to initialize Playwright: {e!s}")
            raise

    async def _block_one_trust_scripts(self, page: Page) -> None:
        """Block OneTrust scripts and third-party resources to prevent bot detection.
        
        OneTrust has sophisticated bot detection that our stealth script can't fully bypass.
        This method blocks OneTrust scripts from loading, preventing bot detection from running,
        AND blocks third-party ad/tracking domains + unneeded resource types (synced from SofaScore).
        Uses a catch-all handler to intercept all requests and abort those to blocked domains.
        """
        one_trust_domains = [
            "onetrust",
            "cookielaw",
            "optanon",
            "cookiepro",
            "trustarc",
        ]

        async def handle_route(route: Route) -> None:
            """Abort requests to OneTrust, ad/tracking domains, and unneeded resource types."""
            url = route.request.url
            url_lower = url.lower()

            # Check if URL contains any OneTrust-related domain
            for domain in one_trust_domains:
                if domain in url_lower:
                    await route.abort()
                    return

            # Block third-party ad/tracking domains (synced from SofaScore)
            from urllib.parse import urlparse
            host = (urlparse(url).hostname or "").lower()
            for domain in BLOCKED_THIRD_PARTY_DOMAINS:
                if host == domain or host.endswith(f".{domain}"):
                    await route.abort()
                    return

            # Block unneeded resource types to save bandwidth (synced from SofaScore)
            if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
                await route.abort()
                return

            # Allow all other requests
            await route.continue_()

        try:
            # Use catch-all pattern to intercept ALL requests
            await page.route("**/*", handle_route)
        except Exception as e:
            self.logger.debug(f"Failed to block scripts: {e}")

    async def _create_context(self, proxy, user_agent, locale, timezone_id, enable_har):
        """Create one browser context. HAR record/replay is applied to the default context only."""
        context_kwargs = {
            "locale": locale,
            "timezone_id": timezone_id,
            "user_agent": user_agent,
            "viewport": {"width": random.randint(1366, 1920), "height": random.randint(768, 1080)},  # noqa: S311
        }
        if proxy is not None:
            context_kwargs["proxy"] = proxy
        if enable_har:
            har_record_path = os.environ.get(HAR_RECORD_ENV_VAR)
            if har_record_path:
                self.logger.info(f"HAR recording mode active: {har_record_path}")
                context_kwargs["record_har_path"] = Path(har_record_path)
                context_kwargs["record_har_mode"] = "full"
                context_kwargs["record_har_url_filter"] = HAR_REPLAY_URL_PATTERN

        context = await self.browser.new_context(**context_kwargs)
        await context.add_init_script(STEALTH_SCRIPT)

        if enable_har:
            har_replay_path = os.environ.get(HAR_REPLAY_ENV_VAR)
            if har_replay_path:
                self.logger.info(f"HAR replay mode active: {har_replay_path}")
                await context.route_from_har(
                    Path(har_replay_path),
                    url=HAR_REPLAY_URL_PATTERN,
                    not_found="abort",
                )
        return context

    def non_default_context_keys(self) -> list[str]:
        """Keys of proxy contexts other than the default one (empty for single/no-proxy)."""
        return [key for key in self.contexts if key != self._default_key]

    async def new_page_on_key(self, key: str):
        """Open a new page in the context bound to a specific proxy key."""
        return await self.contexts[key].new_page()

    async def new_rotated_page(self):
        """Open a page on the next round-robin proxy. Returns (page, proxy_key).

        Raises AllProxiesExhaustedError if every proxy is blacklisted.
        """
        if self._proxy_manager is None:
            return await self.context.new_page(), self._default_key
        entry = self._proxy_manager.next_proxy()
        if entry is None:
            raise AllProxiesExhaustedError("All proxies are blacklisted; cannot open a new page.")
        page = await self.contexts[entry.key].new_page()
        return page, entry.key

    def report_page_result(self, key: str, is_proxy_failure: bool) -> None:
        """Forward a per-page outcome to the proxy pool (no-op without a proxy manager)."""
        if self._proxy_manager is not None:
            self._proxy_manager.report_result(key, is_proxy_failure)

    def blacklist_proxy(self, key: str) -> None:
        """Force a proxy out of rotation (no-op without a proxy manager)."""
        if self._proxy_manager is not None:
            self._proxy_manager.blacklist_proxy(key)

    async def block_one_trust_for_page(self, page: Page) -> None:
        """Block OneTrust scripts and third-party resources for a specific page (call before navigation)."""
        await self._block_one_trust_scripts(page)

    async def humanize_page(self, page: Page) -> None:
        """Simulate human mouse movement and scrolling (synced from SofaScore backfill)."""
        try:
            import random as _rnd
            await page.mouse.move(
                _rnd.randint(140, 640), _rnd.randint(110, 420),
                steps=_rnd.randint(8, 18)
            )
            await page.wait_for_timeout(int(_rnd.uniform(200, 600)))
            await page.mouse.wheel(0, _rnd.randint(180, 520))
            await page.wait_for_timeout(int(_rnd.uniform(400, 1000)))
        except Exception:
            return

    async def cleanup(self):
        """Properly closes Playwright instances."""
        self.logger.info("Cleaning up Playwright resources...")
        if self.page:
            await self.page.close()
        for context in self.contexts.values():
            await context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("Playwright resources cleanup complete.")
