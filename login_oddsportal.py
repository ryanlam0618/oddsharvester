#!/usr/bin/env python3
"""
Script to log in to OddsPortal and save cookies for later use.
This allows scraping leagues that require authentication.
"""

import asyncio
import json
import sys
sys.path.insert(0, '/home/openclaw/.openclaw/workspace/projects/OddsHarvester/src')

from oddsharvester.core.playwright_manager import PlaywrightManager
from oddsharvester.core.browser_helper import BrowserHelper


async def login_to_oddsportal(email: str, password: str, save_path: str = "/home/openclaw/.openclaw/workspace/projects/OddsHarvester/cookies.json"):
    """
    Log in to OddsPortal and save cookies to a file.
    
    Args:
        email: Your OddsPortal email
        password: Your OddsPortal password
        save_path: Where to save the cookies
    """
    print("=" * 70)
    print("OddsPortal Login Script")
    print("=" * 70)
    
    # Initialize
    print("\n[1] Initializing browser...")
    playwright_manager = PlaywrightManager()
    browser_helper = BrowserHelper()
    
    await playwright_manager.initialize(headless=False)  # Show browser for login
    page = await playwright_manager.new_page()
    
    try:
        # Go to login page
        print("\n[2] Going to OddsPortal login page...")
        await page.goto("https://www.oddsportal.com/login/", timeout=60000)
        await page.wait_for_load_state("networkidle")
        
        # Wait for login form
        print("\n[3] Filling in login credentials...")
        await page.wait_for_selector("#login-username1", timeout=10000)
        
        # Fill in username/email
        await page.fill("#login-username1", email)
        await asyncio.sleep(0.5)
        
        # Fill in password
        await page.fill("#login-password1", password)
        await asyncio.sleep(0.5)
        
        # Click login button
        print("\n[4] Clicking login button...")
        await page.click("#login-username1 ~ button[type='submit']")
        
        # Wait for navigation or error
        await asyncio.sleep(5)
        
        # Check if login was successful
        current_url = page.url
        print(f"\n[5] Current URL: {current_url}")
        
        if "login" not in current_url.lower():
            print("✅ Login appears successful!")
            
            # Save cookies
            print(f"\n[6] Saving cookies to {save_path}...")
            cookies = await page.context.cookies()
            
            with open(save_path, 'w') as f:
                json.dump(cookies, f, indent=2)
            
            print(f"✅ Saved {len(cookies)} cookies")
            
            # Show some cookie info
            for cookie in cookies[:5]:
                print(f"   - {cookie['name']}: {cookie['value'][:20]}...")
            
            print(f"\n✅ Login complete! Cookies saved to:")
            print(f"   {save_path}")
            print(f"\nYou can now use these cookies with the scraper.")
            
        else:
            print("❌ Login may have failed. Please check the browser.")
            print("   - Make sure credentials are correct")
            print("   - Complete any CAPTCHA if required")
            print("   - Check for any verification emails from OddsPortal")
        
    except Exception as e:
        print(f"\n❌ Error during login: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        print("\n[7] Closing browser...")
        await playwright_manager.cleanup()


def load_cookies(page) -> list:
    """Load cookies from file and add to page context."""
    save_path = "/home/openclaw/.openclaw/workspace/projects/OddsHarvester/cookies.json"
    
    try:
        with open(save_path, 'r') as f:
            cookies = json.load(f)
        return cookies
    except FileNotFoundError:
        print(f"❌ Cookies file not found: {save_path}")
        return []


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Log in to OddsPortal and save cookies")
    parser.add_argument("--email", "-e", required=True, help="OddsPortal email")
    parser.add_argument("--password", "-p", required=True, help="OddsPortal password")
    parser.add_argument("--output", "-o", default="/home/openclaw/.openclaw/workspace/projects/OddsHarvester/cookies.json",
                        help="Output path for cookies file")
    
    args = parser.parse_args()
    
    asyncio.run(login_to_oddsportal(args.email, args.password, args.output))