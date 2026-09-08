import logging
from unittest.mock import AsyncMock

import pytest

from oddsharvester.core.browser.cookies import CookieDismisser


class TestCookieDismisser:
    @pytest.fixture
    def dismisser(self):
        return CookieDismisser()

    @pytest.mark.asyncio
    async def test_dismiss_cookie_banner_success(self, dismisser, mock_page):
        """Test successful cookie banner dismissal."""
        # Mock successful banner dismissal
        mock_page.wait_for_selector = AsyncMock()
        mock_page.click = AsyncMock()

        result = await dismisser.dismiss(mock_page)
        assert result is True
        mock_page.wait_for_selector.assert_called_once()
        mock_page.click.assert_called_once()

    @pytest.mark.asyncio
    async def test_dismiss_cookie_banner_custom_selector(self, dismisser, mock_page):
        """Test cookie banner dismissal with custom selector."""
        custom_selector = "#custom-cookie-banner"
        mock_page.wait_for_selector = AsyncMock()
        mock_page.click = AsyncMock()

        result = await dismisser.dismiss(mock_page, selector=custom_selector)
        assert result is True
        mock_page.wait_for_selector.assert_called_with(custom_selector, timeout=10000)

    @pytest.mark.asyncio
    async def test_dismiss_cookie_banner_timeout_error(self, dismisser, mock_page):
        """Test cookie banner dismissal when banner is not found (timeout)."""
        mock_page.wait_for_selector.side_effect = TimeoutError("Timeout")

        result = await dismisser.dismiss(mock_page)
        assert result is False

    @pytest.mark.asyncio
    async def test_dismiss_cookie_banner_click_error(self, dismisser, mock_page):
        """Test cookie banner dismissal when click fails."""
        mock_page.wait_for_selector = AsyncMock()
        mock_page.click.side_effect = Exception("Click failed")

        result = await dismisser.dismiss(mock_page)
        assert result is False

    @pytest.mark.asyncio
    async def test_dismiss_cookie_banner_wait_error(self, dismisser, mock_page):
        """Test cookie banner dismissal when wait_for_selector fails."""
        mock_page.wait_for_selector.side_effect = Exception("Wait failed")

        result = await dismisser.dismiss(mock_page)
        assert result is False

    @pytest.mark.asyncio
    async def test_logging_during_cookie_banner_dismissal(self, dismisser, mock_page, caplog):
        """Test logging during cookie banner dismissal."""
        with caplog.at_level(logging.INFO):
            mock_page.wait_for_selector = AsyncMock()
            mock_page.click = AsyncMock()

            await dismisser.dismiss(mock_page)

            assert "Checking for cookie banner" in caplog.text
            assert "Cookie banner found. Dismissing it." in caplog.text

    @pytest.mark.asyncio
    async def test_full_cookie_banner_flow(self, dismisser, mock_page):
        """Test the complete cookie banner dismissal flow."""
        # Mock successful cookie banner dismissal
        mock_page.wait_for_selector = AsyncMock()
        mock_page.click = AsyncMock()

        result = await dismisser.dismiss(mock_page, timeout=5000)
        assert result is True
        mock_page.wait_for_selector.assert_called_once()
        mock_page.click.assert_called_once()
