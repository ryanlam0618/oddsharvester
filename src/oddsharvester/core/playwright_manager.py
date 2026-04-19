import logging
import random

from playwright.async_api import async_playwright, Page

from oddsharvester.utils.constants import PLAYWRIGHT_BROWSER_ARGS, PLAYWRIGHT_BROWSER_ARGS_DOCKER
from oddsharvester.utils.utils import is_running_in_docker

# Comprehensive anti-detection script to hide automation signatures
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

# Default user agents that look like real browsers
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
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

    async def initialize(
        self,
        headless: bool,
        user_agent: str | None = None,
        locale: str | None = None,
        timezone_id: str | None = None,
        proxy: dict[str, str] | None = None,
    ):
        """
        Initialize and start Playwright with a browser and page.

        Args:
            is_webdriver_headless (bool): Whether to start the browser in headless mode.
            proxy (Optional[Dict[str, str]]): Proxy configuration with keys 'server', 'username', and 'password'.
        """
        try:
            self.logger.info("Starting Playwright...")
            self.timezone_id = timezone_id
            self.playwright = await async_playwright().start()

            browser_args = PLAYWRIGHT_BROWSER_ARGS_DOCKER if is_running_in_docker() else PLAYWRIGHT_BROWSER_ARGS
            self.browser = await self.playwright.chromium.launch(headless=headless, args=browser_args, proxy=proxy)

            # Use provided user_agent or random default
            effective_user_agent = user_agent or random.choice(DEFAULT_USER_AGENTS)  # noqa: S311

            self.context = await self.browser.new_context(
                locale=locale,
                timezone_id=timezone_id,
                user_agent=effective_user_agent,
                viewport={"width": random.randint(1366, 1920), "height": random.randint(768, 1080)},  # noqa: S311
            )

            # Add anti-detection script
            await self.context.add_init_script(STEALTH_SCRIPT)

            self.page = await self.context.new_page()
            
            # Block OneTrust scripts to prevent bot detection
            await self._block_one_trust_scripts(self.page)
            
            self.logger.info("Playwright initialized successfully.")

        except Exception as e:
            self.logger.error(f"Failed to initialize Playwright: {e!s}")
            raise

    async def _block_one_trust_scripts(self, page: Page) -> None:
        """Block OneTrust scripts to prevent bot detection.
        
        OneTrust has sophisticated bot detection that our stealth script can't fully bypass.
        This method blocks OneTrust scripts from loading, preventing bot detection from running.
        """
        patterns = [
            "**/*onetrust*",
            "**/*cookielaw*",
            "**/*otSDKStub*",
            "**/*consent*",
        ]
        
        for pattern in patterns:
            try:
                await page.route(pattern, lambda route: route.abort())
            except Exception as e:
                self.logger.debug(f"Failed to block pattern {pattern}: {e}")

    async def block_one_trust_for_page(self, page: Page) -> None:
        """Block OneTrust scripts for a specific page (call before navigation)."""
        await self._block_one_trust_scripts(page)

    async def cleanup(self):
        """Properly closes Playwright instances."""
        self.logger.info("Cleaning up Playwright resources...")
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("Playwright resources cleanup complete.")
