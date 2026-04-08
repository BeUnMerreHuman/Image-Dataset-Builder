# 🖼️ Automated Image Dataset Pipeline

An end-to-end workflow designed to scrape, curate, and package high-quality image datasets. This project automates the transition from raw search keywords to a structured, high-performance `.parquet` dataset, featuring a custom PyQt6 GUI for manual human-in-the-loop curation.

## ✨ Key Features

* **Multi-Source Scraping:** Concurrent scraping from Yandex and Pinterest using **camoufox**.
* **Orchestrated Workflow:** Powered by **Snakemake** for reproducible, resumable and error-resistant data processing.
* **Human-in-the-Loop:** A dedicated **PyQt6 GUI** for rapid manual image selection.
* **Automated Cleaning:** Built-in deduplication and low-quality image filtering.
* **High-Performance Storage:** Outputs to **Apache Parquet** for seamless integration with ML frameworks like PyTorch or TensorFlow.
* **State Persistence:** Supports incremental runs—new data is appended to existing datasets automatically.

---

## 🚀 Quick Start

The entire process is automated via a batch script for Windows users, You can simply double click `DataPipeline.bat` or run the following Terminal commands:

```bash
git clone https://github.com/BeUnMerreHuman/Image-Dataset-Builder.git
cd Image-Dataset-Builder
.\DataPipeline.bat

```

> [!IMPORTANT]
>The script requires that [`uv`](https://pypi.org/project/uv/) and [`Microsoft Visual C++ Redistributable`](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170) be installed.

>The pipeline will automatically open `.env` and `keywords.txt` on every run. Edit, then **Save and close** these files to allow the automation to proceed to the scraping phase.

---

## 📁 Project Structure

```text
.
├── src/
│   ├── Snakefile           # The "brain" of the pipeline (Workflow logic)
│   ├── ImageDownloader.py  # Scraper engine (Yandex & Pinterest)
│   ├── ImageSelector.py    # PyQt6 GUI for manual curation
│   ├── DataCleaner.py      # Filters low-quality & duplicate images
│   ├── MetadataCreator.py  # Generates image metadata & source links
│   ├── LabelEditor.py      # Edit the image label
│   ├── DatasetCreator.py   # Compiles data into final Parquet format
│   ├── DatasetViewer.py    # Final GUI for dataset verification
│   ├── MoveFiles.py        # Utility for file organization/staging
│   └── keywords.txt        # Your search terms (one per line)
├── .env                    # Configuration (Directories, Required Images)
├── DataPipeline.bat        # Windows entry point
├── requirements.txt        # Python dependencies
└── README.md

```

---

## 🧹 Cleanup & Safety

* **Trash System:** Temporary files and logs are moved to `src/Trash/` rather than being deleted immediately, preventing accidental data loss.
* **Backups:** Every time the pipeline appends data to an existing `.parquet` file, a backup of the previous version is generated in the `src/` folder.
