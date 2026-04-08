import os
import csv
import json
import time
import base64
import random
import threading
import queue
import requests
import concurrent.futures
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, quote
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import xxhash
from tqdm import tqdm
from dotenv import load_dotenv

try:
    from camoufox.sync_api import Camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False
    print("WARNING: camoufox not installed. Scrapers requiring JS will fail.")

# ---------------------------------------------------------------------------
# Configuration & Contexts
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

with open("selectors.json", "r", encoding="utf-8") as f:
    SELECTORS = json.load(f)

class Config:
    KEYWORDS_FILE                = "keywords.txt"
    YANDEX_IMAGES_PER_KEYWORD    = int(os.getenv("YANDEX_IMAGES_PER_KEYWORD", 100))
    PINTEREST_IMAGES_PER_KEYWORD = int(os.getenv("PINTEREST_IMAGES_PER_KEYWORD", 100))
    BASE_DOWNLOAD_DIR            = os.getenv("BASE_DOWNLOAD_DIR", "downloads")
    HEADLESS                     = os.getenv("HEADLESS_MODE", "True").lower() in ("true", "1", "t")
    MAX_RETRIES                  = int(os.getenv("MAX_RETRIES", 3))
    MAX_SCROLLS                  = int(os.getenv("MAX_SCROLLS", 30))
    DELAY_BETWEEN_KEYWORDS       = float(os.getenv("DELAY_BETWEEN_KEYWORDS", 3))
    MAX_CONCURRENT_DOWNLOADS     = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", 10))
    BATCH_LOG_SIZE               = 50
    BROWSER_RESTART_INTERVAL     = 20

    BLACKLISTED_DOMAINS = {
        "telegram-cdn.org", "telegram.org", "t.me", "cdn.telegram",
        "avatars.mds.yandex.net", "yastatic.net",
    }
    VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_error(source: str, keyword: str, error_msg: str):
    print(f"\n[{get_timestamp()}] [ERROR] [{source.upper()}] Keyword: '{keyword}' | {error_msg}")

# ---------------------------------------------------------------------------
# Shared Utilities
# ---------------------------------------------------------------------------
def sanitize_folder(keyword: str) -> str:
    s = "".join(c if c.isalnum() or c in (" ", "-", "_") else "" for c in keyword)
    return s.replace(" ", "_") or "unnamed"

def get_extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in Config.VALID_EXTENSIONS:
        if ext in path: return ext
    return ".jpg"

def xxh64_filename(content: bytes, ext: str) -> str:
    digest_bytes = xxhash.xxh64(content).intdigest().to_bytes(8, "big")
    b64 = base64.urlsafe_b64encode(digest_bytes).decode("ascii").rstrip("=")
    return f"{b64}{ext}"

def is_valid_image_url(url: str) -> bool:
    if not url or not url.startswith("http"): return False
    try:
        parsed = urlparse(url)
        if any(b in parsed.netloc.lower() for b in Config.BLACKLISTED_DOMAINS): return False
        has_ext = any(ext in parsed.path.lower() for ext in Config.VALID_EXTENSIONS)
        if not has_ext and len(url) < 50: return False
        return True
    except Exception as e:
        log_error("VALIDATOR", "N/A", f"URL parse failure for {url}: {e}")
        return False

# ---------------------------------------------------------------------------
# Async Data Pipeline (Buffered I/O)
# ---------------------------------------------------------------------------
class AsyncLogger:
    
    def __init__(self):
        self.progress_file = "progress.json"
        self.csv_file = "downloads.csv"
        self.queue = queue.Queue()
        self.progress = self._load_progress()
        self._init_csv()
        
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()

    def _init_csv(self):
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["keyword", "source", "image_number", "image_path", "image_url", "domain", "status", "timestamp"])

    def _load_progress(self) -> dict:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r") as f: return json.load(f)
            except Exception as e:
                log_error("LOGGER", "N/A", f"Failed to load progress file: {e}")
        return {}

    def log_download(self, keyword, source, image_number, image_path, image_url, status):
        domain = urlparse(image_url).netloc if image_url else "unknown"
        record = {
            "type": "csv",
            "data": [keyword, source, image_number, image_path, image_url, domain, status, get_timestamp()]
        }
        self.queue.put(record)
        
        if status == "success":
            self.queue.put({
                "type": "progress",
                "key": f"{source}::{keyword}",
                "count": image_number
            })

    def flush(self):
        """Force write all pending items in the queue."""
        self.queue.put({"type": "flush"})
        self.queue.join()

    def _process_queue(self):
        csv_buffer = []
        while True:
            item = self.queue.get()
            
            if item["type"] == "csv":
                csv_buffer.append(item["data"])
            elif item["type"] == "progress":
                self.progress[item["key"]] = item["count"]
            
            if len(csv_buffer) >= Config.BATCH_LOG_SIZE or item["type"] == "flush":
                if csv_buffer:
                    try:
                        with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
                            csv.writer(f).writerows(csv_buffer)
                        csv_buffer.clear()
                    except Exception as e:
                        log_error("LOGGER", "CSV", f"Failed to flush CSV buffer: {e}")
                try:
                    with open(self.progress_file, "w") as f:
                        json.dump(self.progress, f, indent=2)
                except Exception as e:
                    log_error("LOGGER", "JSON", f"Failed to flush Progress buffer: {e}")
            
            self.queue.task_done()

# ---------------------------------------------------------------------------
# Persistent Browser Manager
# ---------------------------------------------------------------------------
class BrowserManager:
    def __init__(self, headless: bool):
        self.headless = headless
        self.cm = None
        self.browser = None
        self.start()

    def start(self):
        if not CAMOUFOX_AVAILABLE: return
        try:
            self.cm = Camoufox(
                headless=self.headless, geoip=True, humanize=True,
                os=random.choice(["windows", "macos", "linux"]), locale="en-US"
            )
            self.browser = self.cm.__enter__()
        except Exception as e:
            log_error("BROWSER", "INIT", f"Failed to start browser: {e}")

    def stop(self):
        if self.cm:
            try:
                self.cm.__exit__(None, None, None)
            except Exception as e:
                log_error("BROWSER", "STOP", f"Failed to stop browser cleanly: {e}")

    def restart(self):
        self.stop()
        self.start()

    def new_page(self):
        if not self.browser: return None
        try:
            return self.browser.new_page(
                viewport={"width": random.choice([1280, 1366, 1440, 1920]), "height": random.choice([768, 800, 900, 1080])}
            )
        except Exception as e:
            log_error("BROWSER", "NEW_PAGE", f"Failed to create new page: {e}")
            return None

# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------
def download_image(url: str, folder: str, ext: str, referer: str) -> tuple:
    for attempt in range(Config.MAX_RETRIES):
        try:
            resp = requests.get(
                url, timeout=15, stream=True, verify=False,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": referer}
            )
            if resp.status_code == 200:
                content = resp.content
                if content and len(content) > 1000:
                    final_name = xxh64_filename(content, ext)
                    final_path = os.path.join(folder, final_name)
                    if os.path.exists(final_path): return final_path, content
                    os.makedirs(folder, exist_ok=True)
                    with open(final_path, "wb") as fh: fh.write(content)
                    return final_path, content
        except Exception as e:
            error_msg = str(e).lower()
            if "telegram" in error_msg or "cdn" in error_msg: break
            if attempt < Config.MAX_RETRIES - 1: time.sleep(2 ** attempt)
            else: log_error("DOWNLOAD", "N/A", f"Failed fetching {url} after {Config.MAX_RETRIES} attempts: {e}")
    return None, None

def _human_scroll(page, steps: int = 4):
    for _ in range(steps):
        try:
            page.evaluate(f"window.scrollBy(0, {random.randint(200, 500)})")
            page.wait_for_timeout(random.randint(120, 400))
        except Exception as e:
            log_error("SCROLL", "N/A", f"Scroll failed: {e}")
            break

# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------
class BaseScraper:
    def __init__(self, name: str, config: Config, logger: AsyncLogger, browser_manager: BrowserManager):
        self.name = name
        self.config = config
        self.logger = logger
        self.bm = browser_manager
        self.keywords_processed = 0

class YandexScraper(BaseScraper):
    def scrape_urls(self, keyword: str, needed: int) -> list:
        if not CAMOUFOX_AVAILABLE: return []
        urls = set()
        page = self.bm.new_page()
        if not page: return []

        try:
            search_url = f"https://yandex.com/images/search?text={quote(keyword)}"
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(random.randint(2500, 5000))
            
            scrolls = 0
            while len(urls) < needed * 2 and scrolls < self.config.MAX_SCROLLS:
                try:
                    new_urls = page.evaluate(SELECTORS["yandex"]["extract_js"]) or []
                    for u in new_urls:
                        if is_valid_image_url(u): urls.add(u)
                    
                    _human_scroll(page, steps=random.randint(3, 6))
                    scrolls += 1
                    page.evaluate(SELECTORS["yandex"]["more_button_js"])
                except Exception as e:
                    log_error("YANDEX", keyword, f"DOM extraction failed during scroll {scrolls}: {e}")
                    break
        except Exception as e:
            log_error("YANDEX", keyword, f"Page load or navigation failed: {e}")
        finally:
            try: page.close()
            except: pass
            
        return list(urls)[:needed * 2]

    def process(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR, sanitize_folder(keyword))
        target = self.config.YANDEX_IMAGES_PER_KEYWORD
        existing = self.logger.progress.get(f"yandex::{keyword}", 0)
        
        if existing >= target: return
        needed = target - existing
        
        candidates = self.scrape_urls(keyword, needed)
        target_urls = [u for u in candidates if is_valid_image_url(u)][:needed]
        if not target_urls: return

        successful, attempted = existing, 0
        lock = threading.Lock()

        def _worker(url):
            nonlocal successful, attempted
            save_path, _ = download_image(url, folder, get_extension(url), "https://yandex.com/")
            with lock:
                attempted += 1
                if save_path:
                    successful += 1
                    self.logger.log_download(keyword, "yandex", successful, save_path, url, "success")
                else:
                    self.logger.log_download(keyword, "yandex", attempted, "", url, "failed")

        with tqdm(total=needed, desc="  [Yandex] Downloading", unit="img", ncols=80) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_CONCURRENT_DOWNLOADS) as executor:
                futures = []
                for u in target_urls:
                    f = executor.submit(_worker, u)
                    f.add_done_callback(lambda _: pbar.update(1))
                    futures.append(f)
                concurrent.futures.wait(futures)

class PinterestScraper(BaseScraper):
    def scrape_urls(self, keyword: str, needed: int) -> list:
        if not CAMOUFOX_AVAILABLE: return []
        urls = set()
        page = self.bm.new_page()
        if not page: return []

        try:
            search_url = f"https://www.pinterest.com/search/pins/?q={quote(keyword)}&rs=typed"
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(random.randint(3000, 5500))

            scrolls = 0
            while len(urls) < needed * 2 and scrolls < self.config.MAX_SCROLLS:
                try:
                    raw = page.evaluate(SELECTORS["pinterest"]["extract_js"]) or []
                    for u in raw:
                        if "/originals/" not in u:
                            for tok in ["/236x/", "/474x/", "/564x/", "/736x/", "/60x60_RS/"]:
                                u = u.replace(tok, "/originals/")
                        if is_valid_image_url(u): urls.add(u)

                    _human_scroll(page, steps=random.randint(4, 8))
                    scrolls += 1
                    page.evaluate(SELECTORS["pinterest"]["more_button_js"])
                except Exception as e:
                    log_error("PINTEREST", keyword, f"DOM extraction failed during scroll {scrolls}: {e}")
                    break
        except Exception as e:
            log_error("PINTEREST", keyword, f"Page load or navigation failed: {e}")
        finally:
            try: page.close()
            except: pass

        return list(urls)[:needed * 2]

    def process(self, keyword: str):
        folder = os.path.join(self.config.BASE_DOWNLOAD_DIR, sanitize_folder(keyword))
        target = self.config.PINTEREST_IMAGES_PER_KEYWORD
        existing = self.logger.progress.get(f"pinterest::{keyword}", 0)
        
        if existing >= target: return
        needed = target - existing

        candidates = self.scrape_urls(keyword, needed)
        target_urls = [u for u in candidates if is_valid_image_url(u)][:needed]
        if not target_urls: return

        successful, attempted = existing, 0
        lock = threading.Lock()

        def _worker(url):
            nonlocal successful, attempted
            save_path, _ = download_image(url, folder, get_extension(url), "https://www.pinterest.com/")
            with lock:
                attempted += 1
                if save_path:
                    successful += 1
                    self.logger.log_download(keyword, "pinterest", successful, save_path, url, "success")
                else:
                    self.logger.log_download(keyword, "pinterest", attempted, "", url, "failed")

        with tqdm(total=needed, desc="  [Pinterest] Downloading", unit="img", ncols=80) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_CONCURRENT_DOWNLOADS) as executor:
                futures = []
                for u in target_urls:
                    f = executor.submit(_worker, u)
                    f.add_done_callback(lambda _: pbar.update(1))
                    futures.append(f)
                concurrent.futures.wait(futures)

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def main():
    if not os.path.exists(Config.KEYWORDS_FILE):
        log_error("SYSTEM", "INIT", f"Keywords file '{Config.KEYWORDS_FILE}' not found.")
        return

    with open(Config.KEYWORDS_FILE, "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]

    logger = AsyncLogger()
    bm = BrowserManager(Config.HEADLESS)
    
    yandex_scraper = YandexScraper("yandex", Config(), logger, bm)
    pinterest_scraper = PinterestScraper("pinterest", Config(), logger, bm)

    for idx, keyword in enumerate(keywords, 1):
        print(f"\n{'='*70}\n  [Keyword {idx}/{len(keywords)}] {keyword}\n{'='*70}")

        yandex_scraper.process(keyword)
        pinterest_scraper.process(keyword)
        
        logger.flush() 
        
        if idx % Config.BROWSER_RESTART_INTERVAL == 0 and idx < len(keywords):
            print("\n  [SYSTEM] Recycling shared browser context to prevent memory leaks...")
            bm.restart()
            
        if idx < len(keywords): 
            time.sleep(Config.DELAY_BETWEEN_KEYWORDS)

    logger.flush()
    bm.stop()
    print("\nScraping complete. Buffer flushed. Browser terminated.")

if __name__ == "__main__":
    main()
