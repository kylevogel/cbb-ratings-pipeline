"""
Shared helper: fetch a page's HTML with headless Chrome (Selenium).
Used as a fallback when plain `requests` is blocked (e.g. AP 403, ESPN empty page).
One browser is reused across calls and closed automatically at exit.
"""
import atexit
import time

_driver = None


def _get_driver():
    global _driver
    if _driver is None:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        _driver = webdriver.Chrome(options=opts)
        _driver.set_page_load_timeout(45)
        atexit.register(close_driver)
    return _driver


def close_driver():
    global _driver
    if _driver is not None:
        try:
            _driver.quit()
        except Exception:
            pass
        _driver = None


def get_html(url: str, wait_s: float = 4.0) -> str:
    """Load `url` in headless Chrome and return the rendered HTML."""
    driver = _get_driver()
    driver.get(url)
    time.sleep(wait_s)
    return driver.page_source
