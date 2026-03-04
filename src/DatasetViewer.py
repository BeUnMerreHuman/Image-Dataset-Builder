import sys
import os
import shutil
import threading
from pathlib import Path
from collections import OrderedDict
from typing import Set, List, Optional, Tuple, Any

import pandas as pd
from dotenv import load_dotenv

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QScrollArea, QLabel, QPushButton, QComboBox,
    QFrame, QStatusBar, QMessageBox, QGroupBox, QFormLayout, 
    QTableWidget, QTableWidgetItem, QHeaderView
)
from PyQt6.QtCore import (
    Qt, QRunnable, QThreadPool, pyqtSignal, QObject, QTimer,
    QSize, QPoint, pyqtSlot
)
from PyQt6.QtGui import (
    QPixmap, QColor, QCursor, QAction, QKeySequence
)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ─────────────────────────── Configuration ─────────────────────────────────

class Config:
    # Construct path: DATASET_DIR string + ".parquet"
    BASE_DIR = os.getenv("DATASET_DIR", "dataset")
    PARQUET_PATH = Path(f"{BASE_DIR}.parquet")
    
    # UI Constants
    THUMB_SIZE = 160
    GRID_COLS = 5
    PAGE_SIZE = 25  # 5x5
    PIXMAP_CACHE_SIZE = 150

# ─────────────────────────── LRU Pixmap Cache ──────────────────────────────

class PixmapCache:
    """Thread-safe LRU cache for decoded QPixmaps."""
    def __init__(self, maxsize: int = Config.PIXMAP_CACHE_SIZE):
        self._cache: OrderedDict[int, QPixmap] = OrderedDict()
        self._lock = threading.Lock()
        self._max = maxsize

    def get(self, key: int) -> Optional[QPixmap]:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        return None

    def put(self, key: int, pix: QPixmap):
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
    done = pyqtSignal(int, QPixmap)

class ThumbnailLoader(QRunnable):
    """Loads & scales an image from bytes on a worker thread."""

    def __init__(self, row_idx: int, image_bytes: bytes, size: int, signals: _LoadSignals):
        super().__init__()
        self.row_idx = row_idx
        self.image_bytes = image_bytes
        self.size = size
        self.signals = signals
        self.setAutoDelete(True)

    @pyqtSlot()
    def run(self):
        # Check cache first
        pix = PIXMAP_CACHE.get(self.row_idx)
        if pix is None:
            pix = QPixmap()
            # Load from bytes
            if self.image_bytes:
                pix.loadFromData(self.image_bytes)
            
            if not pix.isNull():
                pix = pix.scaled(
                    self.size, self.size,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
                PIXMAP_CACHE.put(self.row_idx, pix)
        
        self.signals.done.emit(self.row_idx, pix)

# ─────────────────────────── Data Layer ────────────────────────────────────

class DataStore:
    """Manages the Parquet DataFrame."""

    def __init__(self):
        self.df: pd.DataFrame = pd.DataFrame()
        self.removal_set: Set[int] = set() # Stores global indices to be removed
        self.unsaved = False
        self.valid = False

    def load(self) -> Tuple[bool, str]:
        path = Config.PARQUET_PATH
        if not path.exists():
            return False, f"File not found: {path}"

        try:
            self.df = pd.read_parquet(path)
            
            cols = [c.lower() for c in self.df.columns]
            if "image" not in cols:
                return False, "Parquet file missing 'image' column."
            # Ensure we have a label column, if strictly named 'label' in requirement
            # If strictly required:
            if "label" not in cols:
                 return False, "Parquet file missing 'label' column."

            # Create a unique index if not present or reset it to be safe
            self.df = self.df.reset_index(drop=True)
            self.valid = True
            self.removal_set.clear()
            PIXMAP_CACHE.clear()
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def get_labels(self) -> List[str]:
        if not self.valid: return []
        return sorted(self.df["label"].unique().astype(str))

    def get_rows_by_label(self, label: str) -> List[int]:
        if not self.valid: return []
        return self.df[self.df["label"].astype(str) == label].index.tolist()

    def get_row_data(self, idx: int) -> dict:
        if idx not in self.df.index: return {}
        row = self.df.loc[idx].to_dict()
        if "image" in row:
            del row["image"]
        return row

    def get_image_bytes(self, idx: int) -> bytes:
        return self.df.at[idx, "image"]

    def mark_for_removal(self, idx: int):
        self.removal_set.add(idx)
        self.unsaved = True

    def unmark_removal(self, idx: int):
        self.removal_set.discard(idx)
        self.unsaved = True

    def toggle_removal(self, idx: int):
        if idx in self.removal_set:
            self.unmark_removal(idx)
        else:
            self.mark_for_removal(idx)

    def save_changes(self) -> Tuple[bool, str]:
        if not self.removal_set:
            return True, "No changes to save."

        try:
            original_path = Config.PARQUET_PATH
            backup_path = original_path.with_suffix(".parquet.bak")

            # 1. Create Backup
            shutil.copy2(original_path, backup_path)

            # 2. Drop rows
            remove_list = list(self.removal_set)
            self.df = self.df.drop(remove_list).reset_index(drop=True)

            # 3. Save new parquet
            self.df.to_parquet(original_path, index=False)

            # 4. Reset state
            self.removal_set.clear()
            self.unsaved = False
            PIXMAP_CACHE.clear() # Indexs shifted, clear cache
            
            return True, f"Removed {len(remove_list)} images. Backup created."
        except Exception as e:
            return False, f"Save failed: {e}"

# ─────────────────────────── Theme ──────────────────────────────────────────

DARK = {
    "bg":        "#0d0f14",
    "surface":   "#161920",
    "surface2":  "#1e2230",
    "border":    "#2a2f42",
    "accent":    "#4f8ef7",
    "danger":    "#f74f7a", # Red for removal
    "text":      "#e8eaf0",
    "subtext":   "#7a829a",
    "selected_bg": "#2d1b20", # Reddish bg for selected
}

STYLESHEET = f"""
QMainWindow, QWidget {{
    background: {DARK['bg']};
    color: {DARK['text']};
    font-family: 'Segoe UI', 'Roboto', sans-serif;
    font-size: 13px;
}}
QScrollArea {{ border: none; background: {DARK['bg']}; }}
QScrollBar:vertical {{
    background: {DARK['surface']}; width: 10px; border-radius: 5px;
}}
QScrollBar::handle:vertical {{
    background: {DARK['border']}; border-radius: 5px; min-height: 20px;
}}
QComboBox, QPushButton {{
    background: {DARK['surface2']};
    border: 1px solid {DARK['border']};
    border-radius: 4px;
    padding: 6px 12px;
    color: {DARK['text']};
}}
QComboBox::drop-down {{ border: none; }}
QPushButton:hover {{ border-color: {DARK['accent']}; color: {DARK['accent']}; }}
QPushButton#danger {{
    background: {DARK['danger']}; color: white; border: none; font-weight: bold;
}}
QPushButton#danger:hover {{ background: #ff6f92; }}
QTableWidget {{
    background: {DARK['surface']};
    gridline-color: {DARK['border']};
    border: 1px solid {DARK['border']};
}}
QHeaderView::section {{
    background: {DARK['surface2']};
    padding: 4px;
    border: none;
    border-bottom: 1px solid {DARK['border']};
    color: {DARK['subtext']};
}}
QLabel#header {{ font-size: 16px; font-weight: bold; color: {DARK['text']}; }}
QLabel#subtext {{ color: {DARK['subtext']}; }}
"""

# ─────────────────────────── Thumbnail Card ────────────────────────────────

class ThumbnailCard(QFrame):
    """
    State Logic:
    - Normal: Border None/Dark. Means "Keep".
    - Selected: Border Red + "To Remove" text. Means "Delete".
    """
    clicked = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.row_idx: int = -1
        self.to_remove: bool = False
        self._pool = QThreadPool.globalInstance()

        self.setFixedSize(Config.THUMB_SIZE + 10, Config.THUMB_SIZE + 30)
        self.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))

        lay = QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.setSpacing(2)

        self.img_lbl = QLabel()
        self.img_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.img_lbl.setFixedSize(Config.THUMB_SIZE, Config.THUMB_SIZE)
        self.img_lbl.setStyleSheet(f"background:{DARK['surface2']}; border-radius:4px;")
        lay.addWidget(self.img_lbl)

        self.status_lbl = QLabel("")
        self.status_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_lbl.setFixedHeight(20)
        self.status_lbl.setStyleSheet("font-size: 11px; font-weight: bold;")
        lay.addWidget(self.status_lbl)

    def load(self, row_idx: int, image_bytes: bytes, to_remove: bool):
        self.row_idx = row_idx
        self.to_remove = to_remove
        self.img_lbl.setText("...")
        self.img_lbl.setPixmap(QPixmap())
        self._apply_style()
        self._start_load(image_bytes)

    def set_removal_state(self, to_remove: bool):
        self.to_remove = to_remove
        self._apply_style()

    def _start_load(self, data):
        # Async load
        sigs = _LoadSignals()
        sigs.done.connect(self._on_loaded, Qt.ConnectionType.QueuedConnection)
        loader = ThumbnailLoader(self.row_idx, data, Config.THUMB_SIZE, sigs)
        loader._signals_ref = sigs # Keep ref
        self._pool.start(loader)

    @pyqtSlot(int, QPixmap)
    def _on_loaded(self, idx, pix):
        if idx == self.row_idx:
            self.img_lbl.setPixmap(pix)

    def _apply_style(self):
        if self.to_remove:
            # RED style (Remove)
            border = f"2px solid {DARK['danger']}"
            bg = DARK['selected_bg']
            self.status_lbl.setText("TO REMOVE")
            self.status_lbl.setStyleSheet(f"color: {DARK['danger']}; font-weight:bold;")
        else:
            # Normal style (Keep)
            border = f"1px solid {DARK['border']}"
            bg = DARK['surface']
            self.status_lbl.setText("")
            self.status_lbl.setStyleSheet("")

        self.setStyleSheet(f"ThumbnailCard {{ background: {bg}; border: {border}; border-radius: 6px; }}")

    def mousePressEvent(self, ev):
        if ev.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self.row_idx)

# ─────────────────────────── Image Grid ────────────────────────────────────

class ImageGrid(QWidget):
    card_clicked = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._cards: List[ThumbnailCard] = []
        self._current_indices: List[int] = [] # Indices for current page
        self._page = 0
        self._store: Optional[DataStore] = None
        self._all_label_indices: List[int] = [] # All indices for current label

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Pagination Controls
        nav = QHBoxLayout()
        self.prev_btn = QPushButton("◀ Prev")
        self.next_btn = QPushButton("Next ▶")
        self.page_lbl = QLabel("Page 1/1")
        self.page_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.prev_btn.clicked.connect(self._prev_page)
        self.next_btn.clicked.connect(self._next_page)
        
        nav.addWidget(self.prev_btn)
        nav.addWidget(self.page_lbl, 1)
        nav.addWidget(self.next_btn)
        layout.addLayout(nav)

        # Scroll/Grid
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        self.grid_layout = QGridLayout(container)
        self.grid_layout.setSpacing(10)
        scroll.setWidget(container)
        layout.addWidget(scroll)

        # Initialize cards
        for i in range(Config.PAGE_SIZE):
            card = ThumbnailCard()
            card.clicked.connect(self.card_clicked)
            card.hide()
            r, c = divmod(i, Config.GRID_COLS)
            self.grid_layout.addWidget(card, r, c)
            self._cards.append(card)

    def set_store(self, store: DataStore):
        self._store = store

    def load_label(self, label: str):
        if not self._store: return
        self._all_label_indices = self._store.get_rows_by_label(label)
        self._page = 0
        self._refresh()

    def refresh_view(self):
        """Redraw current page (useful after bulk select/deselect)."""
        self._refresh()

    def update_single_card(self, row_idx: int):
        """Optimized update for single click."""
        if row_idx in self._current_indices:
            # Find which card holds this index
            try:
                list_pos = self._current_indices.index(row_idx)
                card = self._cards[list_pos]
                is_removed = row_idx in self._store.removal_set
                card.set_removal_state(is_removed)
            except ValueError:
                pass

    def _refresh(self):
        total_items = len(self._all_label_indices)
        total_pages = max(1, (total_items + Config.PAGE_SIZE - 1) // Config.PAGE_SIZE)
        self._page = max(0, min(self._page, total_pages - 1))

        self.page_lbl.setText(f"Page {self._page + 1} / {total_pages}")
        self.prev_btn.setEnabled(self._page > 0)
        self.next_btn.setEnabled(self._page < total_pages - 1)

        start = self._page * Config.PAGE_SIZE
        end = start + Config.PAGE_SIZE
        
        self._current_indices = self._all_label_indices[start:end]

        for i, card in enumerate(self._cards):
            if i < len(self._current_indices):
                idx = self._current_indices[i]
                data = self._store.get_image_bytes(idx)
                to_remove = idx in self._store.removal_set
                card.load(idx, data, to_remove)
                card.show()
            else:
                card.hide()

    def _prev_page(self):
        self._page -= 1
        self._refresh()

    def _next_page(self):
        self._page += 1
        self._refresh()

# ─────────────────────────── Right Sidebar (Details) ──────────────────────

class DetailsPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(280)
        
        lay = QVBoxLayout(self)
        lay.setContentsMargins(10, 10, 10, 10)
        
        lbl = QLabel("Image Details")
        lbl.setObjectName("header")
        lay.addWidget(lbl)
        
        # Table for details
        self.table = QTableWidget()
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["Column", "Value"])
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        lay.addWidget(self.table)

    def show_info(self, data: dict):
        self.table.setRowCount(len(data))
        for i, (k, v) in enumerate(data.items()):
            self.table.setItem(i, 0, QTableWidgetItem(str(k)))
            self.table.setItem(i, 1, QTableWidgetItem(str(v)))

# ─────────────────────────── Left Sidebar (Controls) ──────────────────────

class LeftPanel(QWidget):
    label_changed = pyqtSignal(str)
    remove_clicked = pyqtSignal()
    
    def __init__(self, store: DataStore, parent=None):
        super().__init__(parent)
        self.store = store
        self.setFixedWidth(240)
        
        lay = QVBoxLayout(self)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(15)

        # Header
        lay.addWidget(QLabel("Dataset Curator", objectName="header"))

        # Label Selection
        lay.addWidget(QLabel("Select Label:", objectName="subtext"))
        self.label_combo = QComboBox()
        self.label_combo.currentTextChanged.connect(self.label_changed)
        lay.addWidget(self.label_combo)
        
        # Stats
        self.stats_group = QGroupBox("Selection Stats")
        v = QVBoxLayout()
        self.remove_count_lbl = QLabel("Marked for removal: 0")
        self.remove_count_lbl.setStyleSheet(f"color: {DARK['danger']}; font-weight: bold; font-size: 14px;")
        v.addWidget(self.remove_count_lbl)
        self.stats_group.setLayout(v)
        lay.addWidget(self.stats_group)

        # Actions
        lay.addStretch()
        
        note = QLabel("Note: 'Remove' overwrites the .parquet file. A backup will be created.")
        note.setWordWrap(True)
        note.setObjectName("subtext")
        lay.addWidget(note)

        self.btn_remove = QPushButton("⚠️ Remove Selected Images")
        self.btn_remove.setObjectName("danger")
        self.btn_remove.setMinimumHeight(40)
        self.btn_remove.clicked.connect(self.remove_clicked)
        lay.addWidget(self.btn_remove)

    def populate_labels(self):
        labels = self.store.get_labels()
        self.label_combo.blockSignals(True)
        self.label_combo.clear()
        self.label_combo.addItems(labels)
        self.label_combo.blockSignals(False)
        
        if labels:
            self.label_changed.emit(labels[0])

    def update_stats(self):
        count = len(self.store.removal_set)
        self.remove_count_lbl.setText(f"Marked for removal: {count}")
        
        if count > 0:
            self.btn_remove.setEnabled(True)
            self.btn_remove.setText(f"⚠️ Remove {count} Images")
        else:
            self.btn_remove.setEnabled(False)
            self.btn_remove.setText("Remove Selected Images")

# ─────────────────────────── Main Window ───────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Parquet Image Cleaner")
        self.resize(1300, 850)
        self.setStyleSheet(STYLESHEET)

        self.store = DataStore()

        # Layout Setup
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setSpacing(0)
        root.setContentsMargins(0,0,0,0)

        # 1. Left Panel
        self.left_panel = LeftPanel(self.store)
        self.left_panel.label_changed.connect(self._on_label_changed)
        self.left_panel.remove_clicked.connect(self._on_save_removal)
        root.addWidget(self.left_panel)
        
        # Separator
        sep1 = QFrame(); sep1.setFrameShape(QFrame.Shape.VLine)
        sep1.setStyleSheet(f"color:{DARK['border']}")
        root.addWidget(sep1)

        # 2. Middle Grid
        self.grid = ImageGrid()
        self.grid.set_store(self.store)
        self.grid.card_clicked.connect(self._on_card_clicked)
        root.addWidget(self.grid, 1) # Expand

        # Separator
        sep2 = QFrame(); sep2.setFrameShape(QFrame.Shape.VLine)
        sep2.setStyleSheet(f"color:{DARK['border']}")
        root.addWidget(sep2)

        # 3. Right Details
        self.details = DetailsPanel()
        root.addWidget(self.details)

        # Status Bar
        self.status = QStatusBar()
        self.setStatusBar(self.status)

        # Init
        QTimer.singleShot(0, self._load_data)

    def _load_data(self):
        ok, msg = self.store.load()
        if not ok:
            QMessageBox.critical(self, "Error Loading Parquet", msg)
            return
        
        self.left_panel.populate_labels()
        self.left_panel.update_stats()
        self.status.showMessage(f"Loaded {len(self.store.df)} rows from {Config.PARQUET_PATH}")

    @pyqtSlot(str)
    def _on_label_changed(self, label):
        self.grid.load_label(label)

    @pyqtSlot(int)
    def _on_card_clicked(self, row_idx):
        # 1. Toggle Selection logic
        self.store.toggle_removal(row_idx)
        
        # 2. Update Grid Visuals
        self.grid.update_single_card(row_idx)
        
        # 3. Update Left Sidebar Stats
        self.left_panel.update_stats()
        
        # 4. Update Right Sidebar Details
        row_data = self.store.get_row_data(row_idx)
        self.details.show_info(row_data)

    @pyqtSlot()
    def _on_save_removal(self):
        count = len(self.store.removal_set)
        if count == 0: return

        confirm = QMessageBox.question(
            self, "Confirm Removal",
            f"Are you sure you want to permanently remove {count} images?\n"
            "The file will be overwritten (a .bak file will be created).",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if confirm == QMessageBox.StandardButton.Yes:
            self.status.showMessage("Saving... please wait.")
            QApplication.processEvents() # Flush UI
            
            ok, msg = self.store.save_changes()
            
            if ok:
                self.status.showMessage(msg)
                # Reload UI data
                self._load_data()
            else:
                QMessageBox.critical(self, "Save Failed", msg)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())