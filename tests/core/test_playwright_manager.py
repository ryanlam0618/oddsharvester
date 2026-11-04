from unittest.mock import AsyncMock, patch

import pytest

from oddsharvester.core.exceptions import AllProxiesExhaustedError
from oddsharvester.core.playwright_manager import PlaywrightManager
from oddsharvester.utils.proxy_manager import ProxyManager


@pytest.fixture
def mock_playwright():
    """Mock async_playwright with the persistent-context chain (afb-ODDSH-PATCHRIGHT).

    The Patchright swap launches a PERSISTENT context via
    chromium.launch_persistent_context(user_data_dir, ...) — the browser
    object is not used for context creation any more. The persistent context
    exposes `pages` as a real list (so `pages[0]` yields the mocked page).
    """
    with patch("oddsharvester.core.playwright_manager.async_playwright") as mock_ap:
        playwright = AsyncMock()
        context = AsyncMock()
        page = AsyncMock()

        mock_ap.return_value.start = AsyncMock(return_value=playwright)
        playwright.chromium.launch_persistent_context = AsyncMock(return_value=context)
        context.pages = [page]
        context.browser = AsyncMock()
        context.new_page = AsyncMock(return_value=page)
        context.add_init_script = AsyncMock()
        context.route_from_har = AsyncMock()
        page.evaluate = AsyncMock(return_value="UTC")

        yield {
            "playwright": playwright,
            "context": context,
            "page": page,
        }


@pytest.mark.asyncio
async def test_route_from_har_called_when_env_var_set(mock_playwright, monkeypatch, tmp_path):
    """HAR replay is preserved in the persistent path: context.route_from_har is awaited."""
    har_path = tmp_path / "snapshot.har"
    har_path.write_text("{}")
    monkeypatch.setenv("ODDSHARVESTER_HAR_REPLAY", str(har_path))

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    mock_playwright["context"].route_from_har.assert_awaited_once_with(
        har_path,
        url="**oddsportal.com/**",
        not_found="abort",
    )


@pytest.mark.asyncio
async def test_route_from_har_not_called_when_env_var_unset(mock_playwright, monkeypatch):
    monkeypatch.delenv("ODDSHARVESTER_HAR_REPLAY", raising=False)

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    mock_playwright["context"].route_from_har.assert_not_called()


@pytest.mark.asyncio
async def test_record_har_kwargs_when_record_env_var_set(mock_playwright, monkeypatch, tmp_path):
    """HAR recording maps to launch_persistent_context kwargs in the persistent path."""
    har_path = tmp_path / "snapshot.har"
    monkeypatch.setenv("ODDSHARVESTER_HAR_RECORD", str(har_path))

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    mock_playwright["context"]  # persistent context is the launch return value
    launch_kwargs = mock_playwright["playwright"].chromium.launch_persistent_context.await_args.kwargs
    assert launch_kwargs["record_har_path"] == har_path
    assert launch_kwargs["record_har_mode"] == "full"
    assert launch_kwargs["record_har_url_filter"] == "**oddsportal.com/**"


@pytest.mark.asyncio
async def test_record_har_kwargs_absent_when_env_var_unset(mock_playwright, monkeypatch):
    monkeypatch.delenv("ODDSHARVESTER_HAR_RECORD", raising=False)

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    launch_kwargs = mock_playwright["playwright"].chromium.launch_persistent_context.await_args.kwargs
    assert "record_har_path" not in launch_kwargs
    assert "record_har_mode" not in launch_kwargs
    assert "record_har_url_filter" not in launch_kwargs


@pytest.mark.asyncio
async def test_resolves_system_timezone_when_none_requested(mock_playwright):
    """With no explicit timezone, the effective browser timezone is captured."""
    mock_playwright["page"].evaluate = AsyncMock(return_value="Europe/Paris")

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    assert pm.timezone_id == "Europe/Paris"
    mock_playwright["page"].evaluate.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_timezone_is_not_overridden(mock_playwright):
    """An explicit timezone_id is kept as-is and not re-resolved from the page."""
    pm = PlaywrightManager()
    await pm.initialize(headless=True, timezone_id="Asia/Tokyo")

    assert pm.timezone_id == "Asia/Tokyo"
    mock_playwright["page"].evaluate.assert_not_called()


@pytest.mark.asyncio
async def test_timezone_resolution_failure_falls_back_to_utc(mock_playwright):
    """If the timezone probe raises, fall back to UTC rather than crash."""
    mock_playwright["page"].evaluate = AsyncMock(side_effect=RuntimeError("probe failed"))

    pm = PlaywrightManager()
    await pm.initialize(headless=True)

    assert pm.timezone_id == "UTC"


@pytest.mark.asyncio
async def test_single_persistent_context_when_no_proxy_manager(mock_playwright):
    """No proxy manager -> exactly ONE persistent context (no browser.new_context)."""
    pm = PlaywrightManager()
    await pm.initialize(headless=True)
    mock_playwright["playwright"].chromium.launch_persistent_context.assert_awaited_once()
    assert list(pm.contexts.keys()) == ["direct"]
    assert pm.non_default_context_keys() == []


@pytest.mark.asyncio
async def test_multi_proxy_collapses_to_single_persistent_context(mock_playwright):
    """DOCUMENTED LIMITATION (afb-ODDSH-PATCHRIGHT): the persistent path launches
    ONE context; multi-proxy rotation degrades — the launch carries the pool
    sentinel proxy and rotation keys keep working via new_rotated_page's
    fallback (per-profile dirs are a follow-up design item)."""
    proxy_manager = ProxyManager(proxy_urls=["http://a.example.com:1", "http://b.example.com:2"])
    pm = PlaywrightManager()
    await pm.initialize(headless=True, proxy_manager=proxy_manager)
    launch_kwargs = mock_playwright["playwright"].chromium.launch_persistent_context.await_args.kwargs
    assert launch_kwargs["proxy"] == {"server": "per-context"}
    assert list(pm.contexts.keys()) == ["direct"]
    assert pm.non_default_context_keys() == []


@pytest.mark.asyncio
async def test_new_rotated_page_reports_key(mock_playwright):
    """Rotation still reports the proxy key; the page comes from the
    persistent context (single-profile degradation, documented)."""
    proxy_manager = ProxyManager(proxy_urls=["http://a.example.com:1", "http://b.example.com:2"])
    pm = PlaywrightManager()
    await pm.initialize(headless=True, proxy_manager=proxy_manager)
    _page, key = await pm.new_rotated_page()
    assert key in {"http://a.example.com:1", "http://b.example.com:2"}


@pytest.mark.asyncio
async def test_new_rotated_page_raises_when_exhausted(mock_playwright):
    proxy_manager = ProxyManager(proxy_urls=["http://a.example.com:1", "http://b.example.com:2"])
    pm = PlaywrightManager()
    await pm.initialize(headless=True, proxy_manager=proxy_manager)
    for key in ["http://a.example.com:1", "http://b.example.com:2"]:
        for _ in range(3):
            pm.report_page_result(key, is_proxy_failure=True)
    with pytest.raises(AllProxiesExhaustedError):
        await pm.new_rotated_page()
