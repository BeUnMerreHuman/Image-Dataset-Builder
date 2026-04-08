import os
import csv
import json
import time
import base64
import random
import threading
import requests
import concurrent.futures
from pathlib import Path
from urllib.parse import urlparse, quote
import xxhash
from tqdm import tqdm
from dotenv import load_dotenv

try:
    from camoufox.sync_api import Camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False
    print("WARNING: camoufox not installed. Both Yandex and Pinterest scraping will be skipped.")
    print("  Install with: pip install camoufox[geoip] && python -m camoufox fetch")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

class Config:
    KEYWORDS_FILE                = "keywords.txt"
    YANDEX_IMAGES_PER_KEYWORD    = int(os.getenv("YANDEX_IMAGES_PER_KEYWORD", 100))
    PINTEREST_IMAGES_PER_KEYWORD = int(os.getenv("PINTEREST_IMAGES_PER_KEYWORD", 100))

    BASE_DOWNLOAD_DIR       = os.getenv("BASE_DOWNLOAD_DIR", "downloads")
    HEADLESS                = os.getenv("HEADLESS_MODE", "True").lower() in ("true", "1", "t")
    MAX_RETRIES             = int(os.getenv("MAX_RETRIES", 3))
    MAX_SCROLLS             = int(os.getenv("MAX_SCROLLS", 30))
    DELAY_BETWEEN_KEYWORDS  = float(os.getenv("DELAY_BETWEEN_KEYWORDS", 3))
    MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 10))

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
# Unified CSV / Progress logger (NOW THREAD-SAFE)
# ---------------------------------------------------------------------------

class RunLogger:
    def __init__(self):
        self.progress_file = "progress.json"
        self.csv_file      = "downloads.csv"
        self.lock          = threading.Lock() # Critical for multithreading
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
        with self.lock:
            with open(self.progress_file, "w") as f:
                json.dump(self.progress, f, indent=2)

    def log(self, keyword, source, image_number, image_path, image_url, status):
        try:
            domain = urlparse(image_url).netloc
        except Exception:
            domain = "unknown"
            
        with self.lock:
            with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    keyword, source, image_number, image_path, image_url,
                    domain, status, time.strftime("%Y-%m-%d %H:%M:%S"),
                ])

    def get_progress_key(self, keyword, source):
        return f"{source}::{keyword}"


# ---------------------------------------------------------------------------
# Image downloader
# ---------------------------------------------------------------------------

def download_image(url: str, folder: str, ext: str, referer: str = "https://www.google.com/") -> tuple:
    for attempt in range(Config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
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
# Human-like behaviour helpers
# ---------------------------------------------------------------------------

def _human_delay(min_ms: int = 800, max_ms: int = 2200):
    time.sleep(random.uniform(min_ms / 1000, max_ms / 1000))

def _human_scroll(page, steps: int = 4):
    for _ in range(steps):
        delta = random.randint(200, 500)
        page.evaluate(f"window.scrollBy(0, {delta})")
        page.wait_for_timeout(random.randint(120, 400))

def _make_camoufox_page(headless: bool):
    if not CAMOUFOX_AVAILABLE:
        raise RuntimeError("camoufox is not installed.")

    camoufox_cm = Camoufox(
        headless=headless,
        geoip=True,
        humanize=True,
        os=random.choice(["windows", "macos", "linux"]),
        locale="en-US",
    )
    browser = camoufox_cm.__enter__()
    page = browser.new_page(
        viewport={
            "width":  random.choice([1280, 1366, 1440, 1920]),
            "height": random.choice([768,  800,  900,  1080]),
        }
    )
    return camoufox_cm, page

def _run_in_thread(fn, *args, **kwargs):
    result = [None]
    exc    = [None]
    def target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as e:
            exc[0] = e
    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join()
    if exc[0] is not None:
        raise exc[0]
    return result[0]


# ---------------------------------------------------------------------------
# Yandex Scraper
# ---------------------------------------------------------------------------

class YandexScraper:
    def __init__(self, config: Config, logger: RunLogger):
        self.config = config
        self.logger = logger

    def _browser_session(self, keyword: str, images_needed: int) -> list:
        image_urls: set = set()
        camoufox_cm = None

        try:
            camoufox_cm, page = _make_camoufox_page(self.config.HEADLESS)
            encoded_keyword = quote(keyword)
            search_url = f"https://yandex.com/images/search?text={encoded_keyword}"
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            _human_delay(2500, 5000)
            scroll_count = 0

            with tqdm(total=images_needed, desc="  [Yandex] Scrolling & Scanning",
                      unit="img", leave=False, ncols=80,
                      bar_format='{desc}: {n}/{total} found |{bar}| {elapsed}') as scroll_pbar:

                while len(image_urls) < images_needed * 2 and scroll_count < self.config.MAX_SCROLLS:
                    new_urls = page.evaluate(r"""
                        () => {
                            const urls = new Set();
                            document.querySelectorAll('a[href*="img_url"]').forEach(link => {
                                try {
                                    const m = link.href.match(/img_url=([^&]+)/);
                                    if (m) {
                                        const imgUrl = decodeURIComponent(m[1]);
                                        if (imgUrl.startsWith('http') && imgUrl.match(/\.(jpg|jpeg|png|gif|webp)/i))
                                            urls.add(imgUrl);
                                    }
                                } catch(e) {}
                            });
                            document.querySelectorAll('img').forEach(img => {
                                const src = img.src || img.dataset.src || img.dataset.bem;
                                if (src && src.startsWith('http') && !src.includes('data:image')) {
                                    if (src.length > 60 && !src.includes('avatars.') && !src.includes('yastatic') && !src.includes('favicon'))
                                        urls.add(src);
                                }
                            });
                            try {
                                document.querySelectorAll('script').forEach(script => {
                                    const text = script.textContent;
                                    if (text && text.includes('http')) {
                                        const matches = text.match(/https?:\/\/[^"'\s]+\.(jpg|jpeg|png|gif|webp)/gi);
                                        if (matches)
                                            matches.forEach(url => {
                                                if (url.length > 60 && !url.includes('yastatic') && !url.includes('avatars.'))
                                                    urls.add(url);
                                            });
                                    }
                                });
                            } catch(e) {}
                            return Array.from(urls);
                        }
                    """) or []

                    for url in new_urls:
                        if is_valid_image_url(url):
                            image_urls.add(url)

                    scroll_pbar.n = min(len(image_urls), images_needed)
                    scroll_pbar.refresh()
                    _human_scroll(page, steps=random.randint(3, 6))
                    scroll_count += 1

                    try:
                        page.evaluate("""
                            const btn = document.querySelector('button.more, .button_more, [class*="show-more"]');
                            if (btn) btn.click();
                        """)
                        _human_delay(1500, 3000)
                    except Exception:
                        pass
                    if random.random() < 0.15:
                        _human_delay(3000, 6000)
        except Exception as e:
            print(f"  [Yandex] Browser error: {e}")
        finally:
            if camoufox_cm:
                try:
                    camoufox_cm.__exit__(None, None, None)
                except Exception:
                    pass

        return list(image_urls)[:images_needed * 2]

    def scrape_yandex_images(self, keyword: str, images_needed: int) -> list:
        if not CAMOUFOX_AVAILABLE:
            return []
        return _run_in_thread(self._browser_session, keyword, images_needed)

    def process_keyword(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR, sanitize_folder(keyword))
        os.makedirs(folder, exist_ok=True)

        progress_key = self.logger.get_progress_key(keyword, "yandex")
        existing = self.logger.progress.get(progress_key, 0)
        target = self.config.YANDEX_IMAGES_PER_KEYWORD

        if existing >= target:
            print(f"  [Yandex] '{keyword}' already complete ({existing}/{target})")
            return

        needed = target - existing
        print(f"  [Yandex] Keyword: {keyword} | Need {needed} more images")

        candidates = self.scrape_yandex_images(keyword, needed)
        
        valid_candidates = [url for url in candidates if is_valid_image_url(url)]
        target_urls = valid_candidates[:needed] 

        if not target_urls:
            print(f"  [Yandex] No URLs found for '{keyword}'")
            return

        print(f"  [Yandex] {len(target_urls)} candidate URLs -> downloading concurrently")

        successful = existing
        attempted = 0
        state_lock = threading.Lock() 

        def _download_worker(url):
            nonlocal successful, attempted
            ext = get_extension(url)
            
            time.sleep(random.uniform(0.0, 0.5)) 
            save_path, _ = download_image(url, folder, ext, referer="https://yandex.com/")

            with state_lock:
                attempted += 1
                if save_path:
                    successful += 1
                    pbar.update(1)
                    self.logger.log(keyword, "yandex", successful, save_path, url, "success")
                    self.logger.progress[progress_key] = successful
                    self.logger.save_progress()
                else:
                    self.logger.log(keyword, "yandex", attempted, "", url, "failed")

        with tqdm(total=needed, desc="  [Yandex] Downloading", unit="", ncols=60, bar_format="{desc} |{bar}| {n}/{total}") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_CONCURRENT_DOWNLOADS) as executor:
                futures = [executor.submit(_download_worker, url) for url in target_urls]
                concurrent.futures.wait(futures)

        print(f"  [Yandex] '{keyword}' done: {successful}/{target} images (tried {attempted} URLs)")


# ---------------------------------------------------------------------------
# Pinterest Scraper
# ---------------------------------------------------------------------------

PINTEREST_SIZE_TOKENS = [
    "/236x/", "/474x/", "/564x/", "/736x/",
    "/60x60_RS/", "/75x75_RS/", "/30x30_RS/",
]

class PinterestScraper:
    def __init__(self, config: Config, logger: RunLogger):
        self.config = config
        self.logger = logger

    def _browser_session(self, keyword: str, needed: int) -> list:
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
                const matches = s.textContent.match(/https?:\/\/[^\s"']+pinimg\.com\/[^\s"']+\.(jpg|jpeg|png|webp)/gi);
                if (matches) matches.forEach(u => urls.add(u));
            });
            return Array.from(urls);
        }
        """

        camoufox_cm = None
        try:
            camoufox_cm, page = _make_camoufox_page(self.config.HEADLESS)
            search_url = f"https://www.pinterest.com/search/pins/?q={quote(keyword)}&rs=typed"
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            _human_delay(3000, 5500)
            scroll_count = 0

            with tqdm(total=needed, desc="    Scanning", unit="img", leave=False, ncols=80, bar_format="{desc}: {n}/{total} found |{bar}| {elapsed}") as pbar:
                while len(found) < needed * 2 and scroll_count < self.config.MAX_SCROLLS:
                    raw = page.evaluate(extract_js) or []
                    for u in raw:
                        if any(t in u for t in PINTEREST_SIZE_TOKENS[:3]):
                            continue
                        for tok in PINTEREST_SIZE_TOKENS:
                            u = u.replace(tok, "/originals/")
                        if is_valid_image_url(u):
                            found.add(u)

                    pbar.n = min(len(found), needed)
                    pbar.refresh()
                    _human_scroll(page, steps=random.randint(4, 8))
                    scroll_count += 1

                    try:
                        page.evaluate("""
                            const btn = document.querySelector('button[aria-label*="more"], button.more, [class*="show-more"]');
                            if (btn) btn.click();
                        """)
                        _human_delay(800, 2000)
                    except Exception:
                        pass

                    if random.random() < 0.15:
                        _human_delay(3000, 6000)

        except Exception as e:
            print(f"    Pinterest browser error: {e}")
        finally:
            if camoufox_cm:
                try:
                    camoufox_cm.__exit__(None, None, None)
                except Exception:
                    pass

        return list(found)[:needed * 2]

    def _collect_urls(self, keyword: str, needed: int) -> list:
        if not CAMOUFOX_AVAILABLE:
            return []
        return _run_in_thread(self._browser_session, keyword, needed)

    def process_keyword(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR, sanitize_folder(keyword))
        os.makedirs(folder, exist_ok=True)

        progress_key = self.logger.get_progress_key(keyword, "pinterest")
        existing = self.logger.progress.get(progress_key, 0)
        target = self.config.PINTEREST_IMAGES_PER_KEYWORD

        if existing >= target:
            print(f"  [Pinterest] '{keyword}' already complete ({existing}/{target})")
            return

        needed = target - existing
        print(f"  [Pinterest] Keyword: {keyword} | Need {needed} more images")

        candidates = self._collect_urls(keyword, needed)
        valid_candidates = [url for url in candidates if is_valid_image_url(url)]
        target_urls = valid_candidates[:needed]

        if not target_urls:
            print(f"  [Pinterest] No URLs found for '{keyword}'")
            return

        print(f"  [Pinterest] {len(target_urls)} candidate URLs -> downloading concurrently")

        successful = existing
        attempted = 0
        state_lock = threading.Lock()

        def _download_worker(url):
            nonlocal successful, attempted
            ext = get_extension(url)
            
            time.sleep(random.uniform(0.0, 0.5))
            save_path, _ = download_image(url, folder, ext, referer="https://www.pinterest.com/")

            with state_lock:
                attempted += 1
                if save_path:
                    successful += 1
                    pbar.update(1)
                    self.logger.log(keyword, "pinterest", successful, save_path, url, "success")
                    self.logger.progress[progress_key] = successful
                    self.logger.save_progress()
                else:
                    self.logger.log(keyword, "pinterest", attempted, "", url, "failed")

        with tqdm(total=needed, desc="  [Pinterest] Downloading", unit="", ncols=60, bar_format="{desc} |{bar}| {n}/{total}") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_CONCURRENT_DOWNLOADS) as executor:
                futures = [executor.submit(_download_worker, url) for url in target_urls]
                concurrent.futures.wait(futures)

        print(f"  [Pinterest] '{keyword}' done: {successful}/{target} images (tried {attempted} URLs)")


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
        print("  UNIFIED IMAGE SCRAPER  --  Yandex + Pinterest")
        print("=" * 70)
        print(f"  Keywords file            : {self.config.KEYWORDS_FILE}")
        print(f"  Yandex images/keyword    : {self.config.YANDEX_IMAGES_PER_KEYWORD}")
        print(f"  Pinterest images/keyword : {self.config.PINTEREST_IMAGES_PER_KEYWORD}")
        print(f"  Download directory       : {self.config.BASE_DOWNLOAD_DIR}")
        print(f"  Total keywords           : {len(keywords)}")
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

            # -----------------------------------------------------------------
            # CONCURRENT BROWSER EXECUTION PER KEYWORD
            # -----------------------------------------------------------------
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                y_future = executor.submit(self.yandex.process_keyword, keyword)
                p_future = executor.submit(self.pinterest.process_keyword, keyword)
                
                try:
                    y_future.result()
                except Exception as e:
                    print(f"  [Yandex] Critical Error on '{keyword}': {e}")
                    
                try:
                    p_future.result()
                except Exception as e:
                    print(f"  [Pinterest] Critical Error on '{keyword}': {e}")

            if idx < len(keywords) and self.config.DELAY_BETWEEN_KEYWORDS > 0:
                print(f"\n  Waiting {self.config.DELAY_BETWEEN_KEYWORDS}s...")
                time.sleep(self.config.DELAY_BETWEEN_KEYWORDS)

        print("\n" + "=" * 70)
        print("  DOWNLOAD SUMMARY")
        print("=" * 70)
        total = 0
        y_target = self.config.YANDEX_IMAGES_PER_KEYWORD
        p_target = self.config.PINTEREST_IMAGES_PER_KEYWORD
        for keyword in keywords:
            folder = os.path.join(self.config.BASE_DOWNLOAD_DIR, sanitize_folder(keyword))
            count = count_existing(folder)
            total += count
            combined_target = y_target + p_target
            status = ("Complete" if count >= combined_target else f"Partial ({count}/{combined_target})")
            print(f"  {keyword:<40}  {status}")
        print("=" * 70)
        print(f"  Total images downloaded: {total}")
        print("=" * 70 + "\n")


def main():
    scraper = ImageScraper()
    scraper.run()

if __name__ == "__main__":
    main()