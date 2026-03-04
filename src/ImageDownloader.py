# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import os
import csv
import json
import time
import base64
import requests
from pathlib import Path
from urllib.parse import urlparse, quote

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import xxhash
from tqdm import tqdm
from dotenv import load_dotenv

# botasaurus  — for Yandex browser automation
from botasaurus.browser import browser, Driver

# Playwright (sync) — for Pinterest browser automation
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("WARNING: Playwright not installed. Pinterest scraping will be skipped.")
    print("  Install with: pip install playwright && playwright install chromium")

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    KEYWORDS_FILE                = "keywords.txt"
    YANDEX_IMAGES_PER_KEYWORD    = int(os.getenv("YANDEX_IMAGES_PER_KEYWORD", 100))
    PINTEREST_IMAGES_PER_KEYWORD = int(os.getenv("PINTEREST_IMAGES_PER_KEYWORD", 100))

    BASE_DOWNLOAD_DIR       = os.getenv("BASE_DOWNLOAD_DIR", "downloads")
    HEADLESS                = os.getenv("HEADLESS_MODE", "True").lower() in ("true", "1", "t")
    MAX_RETRIES             = int(os.getenv("MAX_RETRIES", 3))
    MAX_SCROLLS             = int(os.getenv("MAX_SCROLLS", 30))
    DELAY_BETWEEN_KEYWORDS  = float(os.getenv("DELAY_BETWEEN_KEYWORDS", 3))
    DELAY_BETWEEN_DOWNLOADS = float(os.getenv("DELAY_BETWEEN_DOWNLOADS", 0.5))

    BLACKLISTED_DOMAINS = {
        "telegram-cdn.org", "telegram.org", "t.me", "cdn.telegram",
        "avatars.mds.yandex.net", "yastatic.net",
    }

    VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def sanitize_folder(keyword: str) -> str:
    s = "".join(c if c.isalnum() or c in (" ", "-", "_") else "" for c in keyword)
    return s.replace(" ", "_") or "unnamed"


def get_extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in Config.VALID_EXTENSIONS:
        if ext in path:
            return ext
    return ".jpg"


def xxh64_filename(content: bytes, ext: str) -> str:
    digest_bytes = xxhash.xxh64(content).intdigest().to_bytes(8, "big")
    b64 = base64.urlsafe_b64encode(digest_bytes).decode("ascii").rstrip("=")
    return f"{b64}{ext}"


def count_existing(folder: str) -> int:
    if not os.path.exists(folder):
        return 0
    return sum(
        1 for fn in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, fn))
        and os.path.getsize(os.path.join(folder, fn)) > 1000
    )


def is_valid_image_url(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if any(b in domain for b in Config.BLACKLISTED_DOMAINS):
            return False
        path = parsed.path.lower()
        has_ext = any(ext in path for ext in Config.VALID_EXTENSIONS)
        if not has_ext and len(url) < 50:
            return False
        return True
    except Exception:
        return False


def read_keywords(path: str) -> list:
    if not os.path.exists(path):
        print(f"[Error] Keywords file '{path}' not found.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# ---------------------------------------------------------------------------
# Unified CSV / Progress logger  (one file for both sources)
# ---------------------------------------------------------------------------

class RunLogger:
    def __init__(self):
        self.progress_file = "progress.json"
        self.csv_file      = "downloads.csv"
        self.progress      = self._load_progress()
        self._init_csv()

    def _init_csv(self):
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "keyword", "source", "image_number", "image_path", 
                    "image_url", "domain", "status", "timestamp"
                ])

    def _load_progress(self) -> dict:
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                return json.load(f)
        return {}

    def save_progress(self):
        with open(self.progress_file, "w") as f:
            json.dump(self.progress, f, indent=2)


    def log(self, keyword, source, image_number, image_path, image_url, status):
        try:
            domain = urlparse(image_url).netloc
        except Exception:
            domain = "unknown"
        with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                keyword, source, image_number, image_path, image_url,
                domain, status, time.strftime("%Y-%m-%d %H:%M:%S"),
            ])

    def get_progress_key(self, keyword, source):
        return f"{source}::{keyword}"


# ---------------------------------------------------------------------------
# Image downloader — plain requests, sequential
# ---------------------------------------------------------------------------

def download_image(url: str, folder: str, ext: str,
                   referer: str = "https://www.google.com/") -> tuple:
    """
    Download one image. Returns (final_path, content) on success,
    (None, None) on failure. Deduplicates via xxh64 hash filename.
    """
    for attempt in range(Config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/124.0 Safari/537.36",
                    "Referer": referer,
                },
                stream=True,
            )

            if resp.status_code == 200:
                content = resp.content
                if content and len(content) > 1000:
                    final_name = xxh64_filename(content, ext)
                    final_path = os.path.join(folder, final_name)
                    if os.path.exists(final_path):
                        return final_path, content      
                    os.makedirs(folder, exist_ok=True)
                    with open(final_path, "wb") as fh:
                        fh.write(content)
                    if os.path.getsize(final_path) > 1000:
                        return final_path, content
                    os.remove(final_path)
            return None, None

        except Exception as e:
            error_msg = str(e).lower()
            if "telegram" in error_msg or "cdn" in error_msg:
                return None, None
            if attempt < Config.MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return None, None


# ---------------------------------------------------------------------------
# Yandex Scraper  (botasaurus anti-bot browser)
# ---------------------------------------------------------------------------

class YandexScraper:
    def __init__(self, config: Config, logger: RunLogger):
        self.config = config
        self.logger = logger

    def scrape_yandex_images(self, keyword: str, images_needed: int) -> list:
        """Scrape image URLs from Yandex — mirrors proven working logic."""
        use_headless = self.config.HEADLESS

        @browser(
            reuse_driver=False,
            block_images=False,
            headless=use_headless,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            close_on_crash=True
        )
        def scrape_task(driver: Driver, search_keyword):
            try:
                encoded_keyword = quote(search_keyword)
                search_url = f"https://yandex.com/images/search?text={encoded_keyword}"

                driver.get(search_url)
                time.sleep(4)

                image_urls = set()
                scroll_count = 0
                max_scrolls = 20

                with tqdm(total=images_needed, desc="  Scrolling & Scanning",
                          unit="img", leave=False, ncols=80,
                          bar_format='{desc}: {n}/{total} found |{bar}| {elapsed}') as scroll_pbar:

                    while len(image_urls) < images_needed * 2 and scroll_count < max_scrolls:
                        # JS logic to extract URLs 
                        new_urls = driver.run_js(r"""
                            function extractImageUrls() {
                                let urls = new Set();

                                // Strategy 1: Look for links with img_url parameter
                                document.querySelectorAll('a[href*="img_url"]').forEach(link => {
                                    try {
                                        const urlMatch = link.href.match(/img_url=([^&]+)/);
                                        if (urlMatch) {
                                            const imgUrl = decodeURIComponent(urlMatch[1]);
                                            if (imgUrl.startsWith('http') && imgUrl.match(/\.(jpg|jpeg|png|gif|webp)/i)) {
                                                urls.add(imgUrl);
                                            }
                                        }
                                    } catch(e) {}
                                });

                                // Strategy 2: Look for all image elements
                                document.querySelectorAll('img').forEach(img => {
                                    const src = img.src || img.dataset.src || img.dataset.bem;
                                    if (src && src.startsWith('http') && !src.includes('data:image')) {
                                        if (src.length > 60 && !src.includes('avatars.') && !src.includes('yastatic') && !src.includes('favicon')) {
                                            urls.add(src);
                                        }
                                    }
                                });

                                // Strategy 3: Check for JSON data
                                try {
                                    document.querySelectorAll('script').forEach(script => {
                                        const text = script.textContent;
                                        if (text && text.includes('http')) {
                                            const matches = text.match(/https?:\/\/[^"'\s]+\.(jpg|jpeg|png|gif|webp)/gi);
                                            if (matches) {
                                                matches.forEach(url => {
                                                    if (url.length > 60 && !url.includes('yastatic') && !url.includes('avatars.')) {
                                                        urls.add(url);
                                                    }
                                                });
                                            }
                                        }
                                    });
                                } catch(e) {}

                                return Array.from(urls);
                            }
                            return extractImageUrls();
                        """)

                        if new_urls:
                            for url in new_urls:
                                if is_valid_image_url(url):
                                    image_urls.add(url)

                            scroll_pbar.n = min(len(image_urls), images_needed)
                            scroll_pbar.refresh()

                        driver.run_js("window.scrollBy(0, window.innerHeight);")
                        time.sleep(1.5)
                        scroll_count += 1

                        try:
                            driver.run_js("""
                                const showMoreBtn = document.querySelector('button.more, .button_more, [class*="show-more"]');
                                if (showMoreBtn) showMoreBtn.click();
                            """)
                            time.sleep(2)
                        except:
                            pass

                return list(image_urls)[:images_needed * 2]

            except Exception as e:
                print(f"  ⚠ Error during scraping: {e}")
                return []

        return scrape_task(keyword)

    def process_keyword(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR,
                              sanitize_folder(keyword))
        os.makedirs(folder, exist_ok=True)

        progress_key = self.logger.get_progress_key(keyword, "yandex")
        existing = self.logger.progress.get(progress_key, 0)
        target = self.config.YANDEX_IMAGES_PER_KEYWORD

        # Re-count from disk to be safe
        disk_count = count_existing(folder)

        if existing >= target:
            print(f"  [Yandex] ✓ '{keyword}' already complete ({existing}/{target})")
            return

        needed = target - existing
        print(f"  [Yandex] Keyword: {keyword} | Need {needed} more images")

        candidates = self.scrape_yandex_images(keyword, needed)
        if not candidates:
            print(f"  [Yandex] ⚠ No URLs found for '{keyword}'")
            return

        print(f"  [Yandex] ✓ {len(candidates)} candidate URLs → downloading")

        successful = existing
        attempted = 0

        with tqdm(total=needed, desc="  [Yandex] Downloading",
                  unit="", ncols=60,
                  bar_format="{desc} |{bar}| {n}/{total}") as pbar:

            for url in candidates:
                if successful - existing >= needed:
                    break
                if not is_valid_image_url(url):
                    continue

                attempted += 1
                ext = get_extension(url)
                save_path, _ = download_image(url, folder, ext,
                                              referer="https://yandex.com/")

                if save_path:
                    successful += 1
                    pbar.update(1)
                    self.logger.log(keyword, "yandex", successful,
                                   save_path, url, "success")
                    self.logger.progress[progress_key] = successful
                    self.logger.save_progress()
                else:
                    self.logger.log(keyword, "yandex", attempted,
                                   "", url, "failed")

                time.sleep(self.config.DELAY_BETWEEN_DOWNLOADS)

        print(f"  [Yandex] '{keyword}' done: {successful}/{target} images "
              f"(tried {attempted} URLs)")


# ---------------------------------------------------------------------------
# Pinterest Scraper  (Playwright sync browser)
# ---------------------------------------------------------------------------

PINTEREST_SIZE_TOKENS = [
    "/236x/", "/474x/", "/564x/", "/736x/",
    "/60x60_RS/", "/75x75_RS/", "/30x30_RS/",
]

class PinterestScraper:
    def __init__(self, config: Config, logger: RunLogger):
        self.config = config
        self.logger = logger

    def _collect_urls(self, keyword: str, needed: int) -> list:
        if not PLAYWRIGHT_AVAILABLE:
            print("  [Pinterest] Playwright not available — skipping.")
            return []

        found: set = set()

        extract_js = r"""
        () => {
            const urls = new Set();
            document.querySelectorAll('img').forEach(img => {
                ['src','data-src','srcset'].forEach(attr => {
                    const val = img.getAttribute(attr);
                    if (val) {
                        val.split(',').forEach(entry => {
                            const u = entry.trim().split(' ')[0];
                            if (u.startsWith('http') && u.includes('pinimg.com'))
                                urls.add(u);
                        });
                    }
                });
            });
            document.querySelectorAll('[style*="background-image"]').forEach(el => {
                const m = el.style.backgroundImage.match(/url\(["']?(.*?)["']?\)/);
                if (m && m[1].includes('pinimg.com')) urls.add(m[1]);
            });
            document.querySelectorAll('script').forEach(s => {
                const matches = s.textContent.match(
                    /https?:\/\/[^\s"']+pinimg\.com\/[^\s"']+\.(jpg|jpeg|png|webp)/gi
                );
                if (matches) matches.forEach(u => urls.add(u));
            });
            return Array.from(urls);
        }
        """

        try:
            with sync_playwright() as pw:
                browser_instance = pw.chromium.launch(headless=self.config.HEADLESS)
                ctx = browser_instance.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                )
                page = ctx.new_page()

                try:
                    search_url = (f"https://www.pinterest.com/search/pins/"
                                  f"?q={quote(keyword)}&rs=typed")
                    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(4000)

                    scroll_count = 0

                    with tqdm(total=needed, desc="    Scanning",
                              unit="img", leave=False, ncols=80,
                              bar_format="{desc}: {n}/{total} found |{bar}| {elapsed}") as pbar:

                        while len(found) < needed * 2 and scroll_count < self.config.MAX_SCROLLS:
                            raw = page.evaluate(extract_js) or []

                            for u in raw:
                                # Skip small thumbnails
                                if any(t in u for t in PINTEREST_SIZE_TOKENS[:3]):
                                    continue
                                # Upgrade to original size
                                for tok in PINTEREST_SIZE_TOKENS:
                                    u = u.replace(tok, "/originals/")
                                if is_valid_image_url(u):
                                    found.add(u)

                            pbar.n = min(len(found), needed)
                            pbar.refresh()

                            page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                            page.wait_for_timeout(2000)
                            scroll_count += 1

                            try:
                                page.evaluate("""
                                    const btn = document.querySelector(
                                        'button[aria-label*="more"], button.more, [class*="show-more"]'
                                    );
                                    if (btn) btn.click();
                                """)
                                page.wait_for_timeout(1000)
                            except Exception:
                                pass

                finally:
                    try:
                        browser_instance.close()
                    except Exception:
                        pass

        except Exception as e:
            print(f"    ⚠ Pinterest browser error: {e}")
            return []

        return list(found)[:needed * 2]

    def process_keyword(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR,
                              sanitize_folder(keyword))
        os.makedirs(folder, exist_ok=True)

        progress_key = self.logger.get_progress_key(keyword, "pinterest")
        existing = self.logger.progress.get(progress_key, 0)
        target = self.config.PINTEREST_IMAGES_PER_KEYWORD

        if existing >= target:
            print(f"  [Pinterest] ✓ '{keyword}' already complete ({existing}/{target})")
            return

        needed = target - existing
        print(f"  [Pinterest] Keyword: {keyword} | Need {needed} more images")

        candidates = self._collect_urls(keyword, needed)
        if not candidates:
            print(f"  [Pinterest] ⚠ No URLs found for '{keyword}'")
            return

        print(f"  [Pinterest] ✓ {len(candidates)} candidate URLs → downloading")

        successful = existing
        attempted = 0

        with tqdm(total=needed, desc="  [Pinterest] Downloading",
                  unit="", ncols=60,
                  bar_format="{desc} |{bar}| {n}/{total}") as pbar:

            for url in candidates:
                if successful - existing >= needed:
                    break
                if not is_valid_image_url(url):
                    continue

                attempted += 1
                ext = get_extension(url)
                save_path, _ = download_image(url, folder, ext,
                                              referer="https://www.pinterest.com/")

                if save_path:
                    successful += 1
                    pbar.update(1)
                    self.logger.log(keyword, "pinterest", successful,
                                   save_path, url, "success")
                    self.logger.progress[progress_key] = successful
                    self.logger.save_progress()
                else:
                    self.logger.log(keyword, "pinterest", attempted,
                                   "", url, "failed")

                time.sleep(self.config.DELAY_BETWEEN_DOWNLOADS)

        print(f"  [Pinterest] '{keyword}' done: {successful}/{target} images "
              f"(tried {attempted} URLs)")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class ImageScraper:
    def __init__(self):
        self.config = Config()
        self.logger = RunLogger()
        self.yandex = YandexScraper(self.config, self.logger)
        self.pinterest = PinterestScraper(self.config, self.logger)

    def _print_banner(self, keywords):
        print("\n" + "=" * 70)
        print("  UNIFIED IMAGE SCRAPER  —  Yandex + Pinterest")
        print("=" * 70)
        print(f"  Keywords file             : {self.config.KEYWORDS_FILE}")
        print(f"  Yandex images/keyword     : {self.config.YANDEX_IMAGES_PER_KEYWORD}")
        print(f"  Pinterest images/keyword  : {self.config.PINTEREST_IMAGES_PER_KEYWORD}")
        print(f"  Download directory        : {self.config.BASE_DOWNLOAD_DIR}")
        print(f"  Total keywords            : {len(keywords)}")
        print("=" * 70 + "\n")

    def run(self):
        keywords = read_keywords(self.config.KEYWORDS_FILE)
        self._print_banner(keywords)

        if not keywords:
            print("No keywords to process.")
            return

        for idx, keyword in enumerate(keywords, 1):
            print(f"\n{'='*70}")
            print(f"  [Keyword {idx}/{len(keywords)}]  {keyword}")
            print(f"{'='*70}")

            # --- Yandex ---
            try:
                self.yandex.process_keyword(keyword)
            except Exception as e:
                print(f"  [Yandex] ✗ Error on '{keyword}': {e}")
                print(f"  [Yandex]   Skipping to Pinterest…")

            # --- Pinterest ---
            try:
                self.pinterest.process_keyword(keyword)
            except Exception as e:
                print(f"  [Pinterest] ✗ Error on '{keyword}': {e}")
                print(f"  [Pinterest]   Skipping to next keyword…")

            if idx < len(keywords) and self.config.DELAY_BETWEEN_KEYWORDS > 0:
                print(f"\n  ⏳ Waiting {self.config.DELAY_BETWEEN_KEYWORDS}s…")
                time.sleep(self.config.DELAY_BETWEEN_KEYWORDS)

        # ----- Summary -----
        print("\n" + "=" * 70)
        print("  DOWNLOAD SUMMARY")
        print("=" * 70)
        total = 0
        y_target = self.config.YANDEX_IMAGES_PER_KEYWORD
        p_target = self.config.PINTEREST_IMAGES_PER_KEYWORD
        for keyword in keywords:
            folder = os.path.join(self.config.BASE_DOWNLOAD_DIR,
                                  sanitize_folder(keyword))
            count = count_existing(folder)
            total += count
            combined_target = y_target + p_target
            status = ("✓ Complete" if count >= combined_target
                      else f"⚠ Partial ({count}/{combined_target})")
            print(f"  {keyword:<40}  {status}")
        print("=" * 70)
        print(f"  Total images downloaded: {total}")
        print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    scraper = ImageScraper()
    scraper.run()


if __name__ == "__main__":
    main()