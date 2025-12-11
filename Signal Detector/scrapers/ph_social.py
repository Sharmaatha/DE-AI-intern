from playwright.sync_api import sync_playwright, TimeoutError as PlayTimeoutError
import re
import time

SOCIAL_FIELDS = [
    "twitterUrl",
    "linkedinUrl",
    "facebookUrl",
    "instagramUrl",
    "angelListUrl",
    "threadsUrl",
    "mediumUrl",
]


class ProductHuntSocialScraper:
    """
    Playwright scraper for ProductHunt social links.
    """

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 35000,
        wait_after_load_ms: int = 5000,
        user_agent: str | None = None,
    ):
        self.headless = headless
        self.timeout_ms = timeout_ms
        self.wait_after_load_ms = wait_after_load_ms
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        )

    def _extract_from_text(self, text: str) -> dict:
        results = {}
        for f in SOCIAL_FIELDS:
            m = re.search(rf'"{f}"\s*:\s*"([^"]*)"', text)
            results[f] = m.group(1) if m else None
        return results

    def extract_social_links(self, url: str) -> dict:
        social = {f: None for f in SOCIAL_FIELDS}
        found_any = False

        url = url.split("?")[0].rstrip("/")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--disable-web-security",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            context = browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )

            page = context.new_page()

            def handle_response(response):
                nonlocal social, found_any
                try:
                    body = response.text()
                except Exception:
                    return

                if (
                    "twitterUrl" not in body
                    and "linkedinUrl" not in body
                    and "facebookUrl" not in body
                    and "instagramUrl" not in body
                ):
                    return

                extracted = self._extract_from_text(body)
                for k, v in extracted.items():
                    if v and not social.get(k):
                        social[k] = v.split("?")[0].rstrip("/")
                        found_any = True

            page.on("response", handle_response)

            navigated = False
            for attempt in range(1, 3):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    navigated = True
                    break
                except PlayTimeoutError as e:

                    if attempt == 1:
                        time.sleep(1.5)
                        continue
                    else:
                        print(f"✗ Playwright navigation failed for {url}: {e}")
                except Exception as e:
                    if attempt == 1:
                        time.sleep(1.0)
                        continue
                    else:
                        print(f"✗ Playwright navigation exception for {url}: {e}")

            if not navigated:
                try:
                    browser.close()
                except:
                    pass
                return social

            try:

                page.wait_for_selector("h1", timeout=8000)
            except Exception:
                pass

            time.sleep(self.wait_after_load_ms / 1000.0)

            time.sleep(0.8)

            try:
                browser.close()
            except:
                pass

        return social
