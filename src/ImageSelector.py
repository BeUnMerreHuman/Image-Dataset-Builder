"""
Data Cleaner — PyQt6 GUI
========================
Simplified image-curation tool.
  • Loads images directly from the `images/` directory (organized by character subfolders)
  • Displays images in a 5×5 grid with pagination
  • Click to toggle selection (green = selected, red = deselected)
  • Saves only the selected images to CSV files
  • Keyboard shortcuts: Delete/X = drop, K/Space = keep, S = save, ←/→ = navigate pages
"""

import sys
import csv
import os
import threading
from pathlib import Path
from collections import OrderedDict
from typing import Set, List, Optional, Tuple

import pandas as pd

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QScrollArea, QLabel, QPushButton, QComboBox,
    QFrame, QStatusBar, QSizePolicy,
    QMessageBox, QGroupBox
)
from PyQt6.QtCore import (
    Qt, QRunnable, QThreadPool, pyqtSignal, QObject, QTimer,
    QSize, QPoint, pyqtSlot
)
from PyQt6.QtGui import (
    QPixmap, QColor, QPalette, QFont, QAction, QKeySequence,
    QPainter, QBrush, QPen, QCursor, QIcon
)

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ─────────────────────────── Configuration ─────────────────────────────────

class Config:
    IMAGES_DIR    = Path(os.getenv("BASE_DOWNLOAD_DIR", "downloads"))
    SELECTED_CSV  = Path("selected_images.csv")
    PROCESSED_CSV = Path("processed_labels.csv")

    THUMB_SIZE        = 150      # px, square
    GRID_COLS         = 5
    PAGE_SIZE         = 25       # 5×5 grid
    PIXMAP_CACHE_SIZE = 512      # LRU entries

    IMAGE_EXTENSIONS  = {".jpg", ".jpeg", ".png", ".webp"}


# ─────────────────────────── LRU Pixmap Cache ──────────────────────────────

class PixmapCache:
    """Thread-safe LRU cache for decoded QPixmaps."""

    def __init__(self, maxsize: int = Config.PIXMAP_CACHE_SIZE):
        self._cache: OrderedDict[str, QPixmap] = OrderedDict()
        self._lock  = threading.Lock()
        self._max   = maxsize

    def get(self, key: str) -> Optional[QPixmap]:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        return None

    def put(self, key: str, pix: QPixmap):
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self._max:
                    self._cache.popitem(last=False)
            self._cache[key] = pix

    def clear(self):
        with self._lock:
            self._cache.clear()


PIXMAP_CACHE = PixmapCache()

# ─────────────────────────── Async Loader ──────────────────────────────────

class _LoadSignals(QObject):
    done = pyqtSignal(str, QPixmap)


class ThumbnailLoader(QRunnable):
    """Loads & scales an image on a worker thread, then emits a signal."""

    def __init__(self, path: str, size: int, signals: _LoadSignals):
        super().__init__()
        self.path    = path
        self.size    = size
        self.signals = signals
        self.setAutoDelete(True)

    @pyqtSlot()
    def run(self):
        pix = PIXMAP_CACHE.get(self.path)
        if pix is None:
            pix = QPixmap(self.path)
            if not pix.isNull():
                pix = pix.scaled(
                    self.size, self.size,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
                PIXMAP_CACHE.put(self.path, pix)
        self.signals.done.emit(self.path, pix)


# ─────────────────────────── Data Layer ────────────────────────────────────

class DataStore:
    """Owns all data; no Qt dependency."""

    def __init__(self):
        self.metadata:    pd.DataFrame = pd.DataFrame()
        self.allowed_set: Set[str]     = set()
        self.unsaved      = False

    # ── Loaders ─────────────────────────────────────────────────────────────

    def load(self) -> Tuple[bool, str]:
        if not Config.IMAGES_DIR.exists():
            return False, f"Images directory not found: {Config.IMAGES_DIR.resolve()}"

        records = []
        for img_path in sorted(Config.IMAGES_DIR.rglob("*")):
            if img_path.suffix.lower() in Config.IMAGE_EXTENSIONS:
                parts = img_path.relative_to(Config.IMAGES_DIR).parts
                character = parts[0] if len(parts) > 1 else "unknown"
                records.append({
                    "image_path": str(img_path),
                    "character":  character,
                })

        if not records:
            return False, f"No images found in {Config.IMAGES_DIR.resolve()}"

        self.metadata = pd.DataFrame(records)

        if Config.SELECTED_CSV.exists():
            try:
                prev = set(pd.read_csv(Config.SELECTED_CSV)["image_path"])
                self.allowed_set = prev & set(self.metadata["image_path"])
            except Exception:
                self.allowed_set = set()
        else:
            self.allowed_set = set()

        return True, "OK"

    # ── Characters ──────────────────────────────────────────────────────────

    def characters(self) -> List[str]:
        return sorted(self.metadata["character"].unique())

    def char_df(self, character: str) -> pd.DataFrame:
        return self.metadata[self.metadata["character"] == character]

    def processed_chars(self) -> Set[str]:
        if Config.PROCESSED_CSV.exists():
            try:
                return set(pd.read_csv(Config.PROCESSED_CSV)["character"].unique())
            except Exception:
                return set()
        return set()

    def mark_processed(self, character: str):
        new = pd.DataFrame({"character": [character],
                            "timestamp": [pd.Timestamp.now()]})
        mode   = "a" if Config.PROCESSED_CSV.exists() else "w"
        header = not Config.PROCESSED_CSV.exists()
        new.to_csv(Config.PROCESSED_CSV, mode=mode, header=header, index=False)

    # ── Allowed set ─────────────────────────────────────────────────────────

    def allow(self, path: str):
        self.allowed_set.add(path)
        self.unsaved = True

    def drop(self, path: str):
        self.allowed_set.discard(path)
        self.unsaved = True

    def toggle(self, path: str):
        if path in self.allowed_set:
            self.drop(path)
        else:
            self.allow(path)

    def save(self):
        out = self.metadata[
            self.metadata["image_path"].isin(self.allowed_set)
        ][["image_path", "character"]]
        out.to_csv(Config.SELECTED_CSV, index=False)
        self.unsaved = False

    # ── Stats ────────────────────────────────────────────────────────────────

    def char_stats(self, character: str) -> Tuple[int, int]:
        df    = self.char_df(character)
        total = len(df)
        kept  = sum(1 for p in df["image_path"] if p in self.allowed_set)
        return total, kept


# ─────────────────────────── Theme ──────────────────────────────────────────

DARK = {
    "bg":        "#0d0f14",
    "surface":   "#161920",
    "surface2":  "#1e2230",
    "border":    "#2a2f42",
    "accent":    "#4f8ef7",
    "accent2":   "#f74f7a",
    "green":     "#3ddc84",
    "text":      "#e8eaf0",
    "subtext":   "#7a829a",
    "kept":      "#3ddc84",
    "dropped":   "#f74f7a",
}

STYLESHEET = f"""
QMainWindow, QWidget {{
    background: {DARK['bg']};
    color: {DARK['text']};
    font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
    font-size: 12px;
}}
QSplitter::handle {{
    background: {DARK['border']};
    width: 2px;
}}
QScrollArea {{
    border: none;
    background: {DARK['bg']};
}}
QScrollBar:vertical {{
    background: {DARK['surface']};
    width: 8px;
    margin: 0;
    border-radius: 4px;
}}
QScrollBar::handle:vertical {{
    background: {DARK['border']};
    border-radius: 4px;
    min-height: 20px;
}}
QScrollBar::handle:vertical:hover {{
    background: {DARK['accent']};
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{
    background: {DARK['surface']};
    height: 8px;
    border-radius: 4px;
}}
QScrollBar::handle:horizontal {{
    background: {DARK['border']};
    border-radius: 4px;
    min-width: 20px;
}}
QComboBox {{
    background: {DARK['surface2']};
    border: 1px solid {DARK['border']};
    border-radius: 6px;
    padding: 6px 12px;
    color: {DARK['text']};
    selection-background-color: {DARK['accent']};
    min-width: 180px;
}}
QComboBox::drop-down {{
    border: none;
    padding-right: 8px;
}}
QComboBox QAbstractItemView {{
    background: {DARK['surface2']};
    border: 1px solid {DARK['border']};
    selection-background-color: {DARK['accent']};
    color: {DARK['text']};
    outline: none;
}}
QPushButton {{
    background: {DARK['surface2']};
    border: 1px solid {DARK['border']};
    border-radius: 6px;
    padding: 7px 18px;
    color: {DARK['text']};
    font-weight: 600;
}}
QPushButton:hover {{
    background: {DARK['surface']};
    border-color: {DARK['accent']};
    color: {DARK['accent']};
}}
QPushButton:pressed {{
    background: {DARK['accent']};
    color: #fff;
}}
QPushButton#primary {{
    background: {DARK['accent']};
    border-color: {DARK['accent']};
    color: #fff;
}}
QPushButton#primary:hover {{
    background: #6fa0ff;
    border-color: #6fa0ff;
    color: #fff;
}}
QPushButton#danger {{
    background: {DARK['accent2']};
    border-color: {DARK['accent2']};
    color: #fff;
}}
QPushButton#danger:hover {{
    background: #ff6f92;
    border-color: #ff6f92;
    color: #fff;
}}
QPushButton#success {{
    background: {DARK['green']};
    border-color: {DARK['green']};
    color: #000;
}}
QLabel#header {{
    font-size: 15px;
    font-weight: 700;
    color: {DARK['text']};
    letter-spacing: 1px;
}}
QLabel#subtext {{
    color: {DARK['subtext']};
    font-size: 11px;
}}
QLabel#stat {{
    color: {DARK['accent']};
    font-size: 13px;
    font-weight: 700;
}}
QGroupBox {{
    border: 1px solid {DARK['border']};
    border-radius: 8px;
    margin-top: 12px;
    padding-top: 8px;
    font-weight: 700;
    color: {DARK['subtext']};
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
}}
QStatusBar {{
    background: {DARK['surface']};
    border-top: 1px solid {DARK['border']};
    color: {DARK['subtext']};
    font-size: 11px;
    padding: 2px 8px;
}}
"""


# ─────────────────────────── Thumbnail Card ────────────────────────────────

class ThumbnailCard(QFrame):
    """Single image card in the grid. Click to toggle selected/dropped."""

    clicked = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.meta_idx:   int  = -1
        self.img_path:   str  = ""
        self.is_allowed: bool = True
        self._pool = QThreadPool.globalInstance()

        self.setFixedSize(Config.THUMB_SIZE + 12, Config.THUMB_SIZE + 36)
        self.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))

        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(3)

        self.img_lbl = QLabel(alignment=Qt.AlignmentFlag.AlignCenter)
        self.img_lbl.setFixedSize(Config.THUMB_SIZE, Config.THUMB_SIZE)
        self.img_lbl.setStyleSheet(
            f"background:{DARK['surface2']}; border-radius:4px;"
        )
        lay.addWidget(self.img_lbl)

        self.state_lbl = QLabel(alignment=Qt.AlignmentFlag.AlignCenter)
        self.state_lbl.setFixedHeight(18)
        lay.addWidget(self.state_lbl)

        self._apply_style()

    def load(self, meta_idx: int, img_path: str, allowed: bool):
        self.meta_idx   = meta_idx
        self.img_path   = img_path
        self.is_allowed = allowed
        self.img_lbl.setText("…")
        self.img_lbl.setPixmap(QPixmap())
        self._apply_style()
        self._start_load()

    def set_allowed(self, v: bool):
        self.is_allowed = v
        self._apply_style()

    def _start_load(self):
        cached = PIXMAP_CACHE.get(self.img_path)
        if cached:
            self._on_loaded(self.img_path, cached)
            return
        sigs = _LoadSignals()
        sigs.done.connect(self._on_loaded, Qt.ConnectionType.QueuedConnection)
        job = ThumbnailLoader(self.img_path, Config.THUMB_SIZE, sigs)
        job._signals_ref = sigs
        self._pool.start(job)

    @pyqtSlot(str, QPixmap)
    def _on_loaded(self, path: str, pix: QPixmap):
        if path == self.img_path:
            self.img_lbl.setText("")
            self.img_lbl.setPixmap(pix)

    def _apply_style(self):
        if self.is_allowed:
            border = f"2px solid {DARK['green']}"
            bg     = DARK['surface']
            lbl    = "✓ Selected"
            color  = DARK['green']
        else:
            border = f"2px solid {DARK['dropped']}"
            bg     = "#1a0a0d"
            lbl    = "✕ Dropped"
            color  = DARK['dropped']

        self.setStyleSheet(f"""
            ThumbnailCard {{
                background: {bg};
                border: {border};
                border-radius: 6px;
            }}
        """)
        self.state_lbl.setStyleSheet(f"color:{color}; font-size:10px;")
        self.state_lbl.setText(lbl)

    def mousePressEvent(self, ev):
        if ev.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self.meta_idx)
        super().mousePressEvent(ev)


# ─────────────────────────── Image Grid Widget ─────────────────────────────

class ImageGrid(QWidget):
    """5×5 paginated card grid."""

    card_clicked = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._cards:    List[ThumbnailCard]   = []
        self._page      = 0
        self._page_size = Config.PAGE_SIZE
        self._items:    List[Tuple[int, str]] = []
        self._store:    Optional[DataStore]   = None

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(4)

        # ── Nav bar ──────────────────────────────────────────────────────────
        nav = QHBoxLayout()
        self.prev_btn = QPushButton("◀ Prev")
        self.prev_btn.setFixedWidth(80)
        self.next_btn = QPushButton("Next ▶")
        self.next_btn.setFixedWidth(80)
        self.page_lbl = QLabel("Page 1 / 1",
                               alignment=Qt.AlignmentFlag.AlignCenter)
        self.page_lbl.setObjectName("subtext")
        nav.addWidget(self.prev_btn)
        nav.addWidget(self.page_lbl, 1)
        nav.addWidget(self.next_btn)
        root.addLayout(nav)

        self.prev_btn.clicked.connect(self._go_prev)
        self.next_btn.clicked.connect(self._go_next)

        # ── Scroll area with 5×5 grid ────────────────────────────────────────
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        inner = QWidget()
        self._grid = QGridLayout(inner)
        self._grid.setSpacing(6)
        scroll.setWidget(inner)
        root.addWidget(scroll, 1)

        # Pre-allocate 25 cards (5×5)
        for i in range(Config.PAGE_SIZE):
            card = ThumbnailCard()
            card.clicked.connect(self.card_clicked)
            card.hide()
            row, col = divmod(i, Config.GRID_COLS)
            self._grid.addWidget(card, row, col)
            self._cards.append(card)

    def set_store(self, store: DataStore):
        self._store = store

    def populate(self, items: List[Tuple[int, str]]):
        """items = list of (meta_idx, image_path)"""
        self._items = items
        self._page  = 0
        self._refresh()

    def refresh_card(self, meta_idx: int):
        """Re-apply visual state for a single card."""
        if self._store is None:
            return
        orig_path = self._store.metadata.iloc[meta_idx]["image_path"]
        for card in self._cards:
            if card.meta_idx == meta_idx:
                card.set_allowed(orig_path in self._store.allowed_set)
                break

    def full_refresh(self):
        if self._store is None:
            return
        for card in self._cards:
            if card.meta_idx >= 0 and card.img_path:
                orig_path = self._store.metadata.iloc[card.meta_idx]["image_path"]
                card.set_allowed(orig_path in self._store.allowed_set)

    def _refresh(self):
        if not self._items or self._store is None:
            for c in self._cards:
                c.hide()
            return

        total_pages = max(1, (len(self._items) + self._page_size - 1) // self._page_size)
        self._page  = max(0, min(self._page, total_pages - 1))
        self.page_lbl.setText(f"Page {self._page + 1} / {total_pages}")
        self.prev_btn.setEnabled(self._page > 0)
        self.next_btn.setEnabled(self._page < total_pages - 1)

        start   = self._page * self._page_size
        visible = self._items[start: start + self._page_size]

        for i, card in enumerate(self._cards):
            if i < len(visible):
                meta_idx, path = visible[i]
                orig_path = self._store.metadata.iloc[meta_idx]["image_path"]
                allowed = orig_path in self._store.allowed_set
                card.load(meta_idx, path, allowed)
                card.show()
            else:
                card.meta_idx = -1
                card.img_path = ""
                card.hide()

    def _go_prev(self):
        self._page -= 1
        self._refresh()

    def _go_next(self):
        self._page += 1
        self._refresh()


# ─────────────────────────── Sidebar ───────────────────────────────────────

class SidePanel(QWidget):
    char_changed   = pyqtSignal(str)
    save_requested = pyqtSignal()
    mark_done      = pyqtSignal(str)
    allow_all      = pyqtSignal()
    drop_all       = pyqtSignal()

    def __init__(self, store: DataStore, parent=None):
        super().__init__(parent)
        self.store = store
        self.setFixedWidth(230)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(10)

        # ── Title ─────────────────────────────────────────────────────────────
        title = QLabel("Data Cleaner")
        title.setObjectName("header")
        lay.addWidget(title)

        sep = QFrame(); sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color:{DARK['border']};")
        lay.addWidget(sep)

        # ── Character selector ───────────────────────────────────────────────
        lay.addWidget(QLabel("Character", objectName="subtext"))
        self.char_box = QComboBox()
        self.char_box.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.char_box.currentTextChanged.connect(self._on_char_changed)
        lay.addWidget(self.char_box)

        # ── Stats ─────────────────────────────────────────────────────────────
        self.stat_lbl = QLabel("Total: – | Kept: – | Removed: –")
        self.stat_lbl.setObjectName("subtext")
        self.stat_lbl.setWordWrap(True)
        lay.addWidget(self.stat_lbl)

        sep2 = QFrame(); sep2.setFrameShape(QFrame.Shape.HLine)
        sep2.setStyleSheet(f"color:{DARK['border']};")
        lay.addWidget(sep2)

        # ── Bulk ops ─────────────────────────────────────────────────────────
        lay.addWidget(QLabel("Bulk actions", objectName="subtext"))

        self.allow_all_btn = QPushButton("✅  Select All")
        self.allow_all_btn.setObjectName("success")
        self.allow_all_btn.clicked.connect(self.allow_all)
        lay.addWidget(self.allow_all_btn)

        self.drop_all_btn = QPushButton("❌  Drop All")
        self.drop_all_btn.setObjectName("danger")
        self.drop_all_btn.clicked.connect(self.drop_all)
        lay.addWidget(self.drop_all_btn)

        sep3 = QFrame(); sep3.setFrameShape(QFrame.Shape.HLine)
        sep3.setStyleSheet(f"color:{DARK['border']};")
        lay.addWidget(sep3)

        # ── Save ─────────────────────────────────────────────────────────────
        self.save_btn = QPushButton("💾  Save to Disk")
        self.save_btn.setEnabled(False)
        self.save_btn.clicked.connect(self.save_requested)
        lay.addWidget(self.save_btn)

        self.done_btn = QPushButton("🏁  Mark Character Done")
        self.done_btn.clicked.connect(
            lambda: self.mark_done.emit(self.current_char()))
        lay.addWidget(self.done_btn)

        lay.addStretch()

        # ── Keyboard hint ────────────────────────────────────────────────────
        hint = QLabel(
            "Shortcuts\n"
            "  Click  — toggle select\n"
            "  S / Ctrl+S  — save\n"
            "  ← / → — prev / next page",
            objectName="subtext"
        )
        hint.setWordWrap(True)
        lay.addWidget(hint)

    def populate_chars(self):
        done = self.store.processed_chars()
        self.char_box.blockSignals(True)
        self.char_box.clear()
        for c in self.store.characters():
            label = f"✅ {c}" if c in done else c
            self.char_box.addItem(label, c)
        self.char_box.blockSignals(False)
        self.char_box.setCurrentIndex(0)
        self._on_char_changed(self.char_box.currentText())

    def current_char(self) -> str:
        idx = self.char_box.currentIndex()
        if idx < 0:
            return ""
        return self.char_box.itemData(idx)

    def update_stats(self):
        c = self.current_char()
        if not c:
            return
        total, kept = self.store.char_stats(c)
        self.stat_lbl.setText(
            f"Total <b>{total}</b> | Kept <b style='color:{DARK['green']}'>{kept}</b>"
            f" | Removed <b style='color:{DARK['dropped']}'>{total - kept}</b>"
        )
        self.save_btn.setEnabled(self.store.unsaved)
        self.save_btn.setStyleSheet(
            f"background:{DARK['accent2']}; color:#fff;" if self.store.unsaved else ""
        )

    def _on_char_changed(self, text: str):
        c = self.current_char()
        if c:
            self.char_changed.emit(c)


# ─────────────────────────── Main Window ───────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self, store: DataStore):
        super().__init__()
        self.store = store
        self._current_char = ""
        self.setWindowTitle("🖼 Data Cleaner")
        self.resize(1100, 820)
        self.setStyleSheet(STYLESHEET)

        # ── Status bar ───────────────────────────────────────────────────────
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self._status_msg("Ready — click images to toggle selection.")

        # ── Central layout ───────────────────────────────────────────────────
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Sidebar
        self.side = SidePanel(store)
        root.addWidget(self.side)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet(f"color:{DARK['border']};")
        root.addWidget(sep)

        # Right pane
        right = QWidget()
        right_lay = QVBoxLayout(right)
        right_lay.setContentsMargins(10, 8, 10, 4)
        right_lay.setSpacing(6)
        root.addWidget(right, 1)

        # ── Info label ───────────────────────────────────────────────────────
        self.mode_lbl = QLabel("")
        self.mode_lbl.setObjectName("subtext")
        right_lay.addWidget(self.mode_lbl)

        # ── Image grid ───────────────────────────────────────────────────────
        self.grid = ImageGrid()
        self.grid.set_store(store)
        self.grid.card_clicked.connect(self._on_card_clicked)
        right_lay.addWidget(self.grid, 1)

        # ── Wire sidebar signals ──────────────────────────────────────────────
        self.side.char_changed.connect(self._on_char_changed)
        self.side.save_requested.connect(self._save)
        self.side.mark_done.connect(self._mark_done)
        self.side.allow_all.connect(self._allow_all)
        self.side.drop_all.connect(self._drop_all)

        # ── Keyboard shortcuts ────────────────────────────────────────────────
        for key in ("S", "Ctrl+S"):
            a = QAction(self)
            a.setShortcut(QKeySequence(key))
            a.triggered.connect(self._save)
            self.addAction(a)

        # Arrow keys for page navigation
        prev_a = QAction(self)
        prev_a.setShortcut(QKeySequence(Qt.Key.Key_Left))
        prev_a.triggered.connect(self.grid._go_prev)
        self.addAction(prev_a)

        next_a = QAction(self)
        next_a.setShortcut(QKeySequence(Qt.Key.Key_Right))
        next_a.triggered.connect(self.grid._go_next)
        self.addAction(next_a)

        # ── Boot ─────────────────────────────────────────────────────────────
        self.side.populate_chars()

    # ── Slots ─────────────────────────────────────────────────────────────────

    @pyqtSlot(str)
    def _on_char_changed(self, char: str):
        self._current_char = char
        self._reload_browse()
        self.side.update_stats()

    def _reload_browse(self):
        df    = self.store.char_df(self._current_char)
        items = [(int(idx), row["image_path"])
                 for idx, row in df.iterrows()
                 if Path(row["image_path"]).exists()]
        self.grid.populate(items)
        self.mode_lbl.setText(
            f"{len(items)} images for '{self._current_char}'")

    @pyqtSlot(int)
    def _on_card_clicked(self, meta_idx: int):
        path = self.store.metadata.iloc[meta_idx]["image_path"]
        self.store.toggle(path)
        self.grid.refresh_card(meta_idx)
        self.side.update_stats()

    @pyqtSlot()
    def _allow_all(self):
        df = self.store.char_df(self._current_char)
        for path in df["image_path"]:
            self.store.allow(path)
        self.grid.full_refresh()
        self.side.update_stats()
        self._status_msg(f"✅ Selected all images for '{self._current_char}'")

    @pyqtSlot()
    def _drop_all(self):
        df = self.store.char_df(self._current_char)
        for path in df["image_path"]:
            self.store.drop(path)
        self.grid.full_refresh()
        self.side.update_stats()
        self._status_msg(f"❌ Dropped all images for '{self._current_char}'")

    @pyqtSlot()
    def _save(self):
        if not self.store.unsaved:
            self._status_msg("Nothing new to save.")
            return
        self.store.save()
        self.side.update_stats()
        self._status_msg(
            f"✓ Saved {len(self.store.allowed_set)} selected images → {Config.SELECTED_CSV}")

    @pyqtSlot(str)
    def _mark_done(self, char: str):
        self.store.mark_processed(char)
        self.side.populate_chars()
        self._status_msg(f"✅ Marked '{char}' as processed.")

    def _status_msg(self, msg: str):
        self.status.showMessage(msg)

    def closeEvent(self, ev):
        if self.store.unsaved:
            r = QMessageBox.question(
                self, "Unsaved changes",
                "You have unsaved changes. Save before exiting?",
                QMessageBox.StandardButton.Save |
                QMessageBox.StandardButton.Discard |
                QMessageBox.StandardButton.Cancel,
            )
            if r == QMessageBox.StandardButton.Save:
                self.store.save()
            elif r == QMessageBox.StandardButton.Cancel:
                ev.ignore()
                return
        ev.accept()


# ─────────────────────────── Entry Point ───────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Data Cleaner")

    # ── Splash ──────────────────────────────────────────────────────────────
    splash = QLabel(
        "Data Cleaner\nLoading images…",
        alignment=Qt.AlignmentFlag.AlignCenter,
    )
    splash.setStyleSheet(
        f"background:{DARK['bg']}; color:{DARK['text']}; font-size:22px; "
        f"font-family: monospace; border: 2px solid {DARK['accent']}; padding:40px;"
    )
    splash.setWindowFlags(
        Qt.WindowType.SplashScreen | Qt.WindowType.FramelessWindowHint)
    splash.resize(420, 180)
    screen = QApplication.primaryScreen().geometry()
    splash.move(screen.center() - QPoint(210, 90))
    splash.show()
    app.processEvents()

    store = DataStore()
    ok, msg = store.load()

    splash.close()

    if not ok:
        QMessageBox.critical(None, "Load Error", msg)
        sys.exit(1)

    win = MainWindow(store)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()