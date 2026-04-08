import os
import shutil
import argparse
import io
import numpy as np
from pathlib import Path
from PIL import Image
import imagehash
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

HAMMING_THRESHOLD = int(os.getenv("HAMMING_THRESHOLD", 12))
MIN_WIDTH = int(os.getenv("MIN_WIDTH", 250))
MIN_HEIGHT = int(os.getenv("MIN_HEIGHT", 250))
VALID_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

TRASH_FOLDER = Path.cwd() / "Trash"

def extract_hash_and_size(img_data):

    try:
        is_path = isinstance(img_data, (str, Path))
        source = img_data if is_path else io.BytesIO(img_data)
        
        with Image.open(source) as img:
            width, height = img.size
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                img = img.convert('RGBA')
                bg = Image.new('RGBA', img.size, (255, 255, 255, 255))
                img = Image.alpha_composite(bg, img)
            img = img.convert('RGB')
            
            phash = imagehash.phash(img)
            hash_array = phash.hash.flatten()
            
            if is_path:
                size = Path(img_data).stat().st_size
            else:
                size = len(img_data)
                
            return width, height, size, hash_array
    except Exception:
        return None, None, None, None


def vectorized_deduplication(records):
    
    print("\nRunning vectorized deduplication analysis...")
    trash_identifiers = set()
    
    if not records:
        return trash_identifiers

    unique_hashes = np.empty((0, 64), dtype=bool)
    unique_metadata = []

    for current_id, current_size, current_hash in records:
        if len(unique_hashes) == 0:
            unique_hashes = np.vstack([unique_hashes, current_hash])
            unique_metadata.append((current_id, current_size))
            continue

        distances = np.count_nonzero(unique_hashes != current_hash, axis=1)
        min_idx = int(np.argmin(distances))

        if distances[min_idx] <= HAMMING_THRESHOLD:
            existing_id, existing_size = unique_metadata[min_idx]
            
            if current_size > existing_size:
                trash_identifiers.add(existing_id)
                unique_metadata[min_idx] = (current_id, current_size)
                unique_hashes[min_idx] = current_hash
            else:
                trash_identifiers.add(current_id)
        else:
            unique_hashes = np.vstack([unique_hashes, current_hash])
            unique_metadata.append((current_id, current_size))

    return trash_identifiers


def process_directory(target_path):
    print(f"Scanning directory: {target_path}")
    image_paths = [
        f for f in Path(target_path).rglob('*')
        if f.is_file() and f.suffix.lower() in VALID_EXTENSIONS
    ]
    
    if not image_paths:
        print("No images found.")
        return

    records = []
    low_res_trash = set()

    for img_path in image_paths:
        w, h, size, hash_arr = extract_hash_and_size(img_path)
        if hash_arr is None:
            continue
        
        if w < MIN_WIDTH or h < MIN_HEIGHT:
            low_res_trash.add(img_path)
        else:
            records.append((img_path, size, hash_arr))

    dup_trash = vectorized_deduplication(records)
    total_trash = low_res_trash.union(dup_trash)

    print(f"\nAnalysis complete.")
    print(f"Low-resolution: {len(low_res_trash)}")
    print(f"Near-duplicates: {len(dup_trash)}")

    if not total_trash:
        print("Nothing to move.")
        return

    TRASH_FOLDER.mkdir(exist_ok=True)
    moved = 0
    for file_path in total_trash:
        try:
            dest = TRASH_FOLDER / file_path.name
            if not dest.exists():
                shutil.move(str(file_path), dest)
                moved += 1
        except Exception as e:
            print(f"Failed to move {file_path.name}: {e}")

    print(f"Moved {moved} files to Trash.")


def process_parquet(target_path):
    print(f"Scanning Parquet file: {target_path}")
    parquet_file = pq.ParquetFile(target_path)
    
    records = []
    low_res_trash = set()
    row_index = 0

    print("Extracting hashes from Parquet batches...")
    for batch in parquet_file.iter_batches(batch_size=1000):
        df = batch.to_pandas()
        
        # Assumption: The column is named 'image' and contains bytes or a dict with 'bytes'
        for _, row in df.iterrows():
            img_data = row['image']
            if isinstance(img_data, dict) and 'bytes' in img_data:
                img_data = img_data['bytes']
                
            w, h, size, hash_arr = extract_hash_and_size(img_data)
            
            if hash_arr is None:
                row_index += 1
                continue
                
            if w < MIN_WIDTH or h < MIN_HEIGHT:
                low_res_trash.add(row_index)
            else:
                records.append((row_index, size, hash_arr))
            
            row_index += 1

    dup_trash = vectorized_deduplication(records)
    total_trash = low_res_trash.union(dup_trash)

    print(f"\nAnalysis complete. Rows scanned: {row_index}")
    print(f"Low-resolution rows: {len(low_res_trash)}")
    print(f"Near-duplicate rows: {len(dup_trash)}")

    if not total_trash:
        print("No duplicates or low-res images. File is already clean.")
        return

    print(f"\nRewriting clean Parquet file to: {target_path}")
    
    writer = None
    current_row_idx = 0
    
    for batch in parquet_file.iter_batches(batch_size=1000):
        df = batch.to_pandas()
        batch_indices = range(current_row_idx, current_row_idx + len(df))
        
        mask = [idx not in total_trash for idx in batch_indices]
        filtered_df = df[mask]
        
        if len(filtered_df) > 0:
            table = pa.Table.from_pandas(filtered_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(target_path, table.schema)
            writer.write_table(table)
            
        current_row_idx += len(df)

    if writer:
        writer.close()
    print("Rewrite complete.")


def main():
    parser = argparse.ArgumentParser(description="Image deduplication and resolution filtering engine.")
    parser.add_argument("target", type=str, help="Path to the target directory or Parquet file.")
    args = parser.parse_args()

    target_path = args.target

    if not os.path.exists(target_path):
        print(f"Error: Target path '{target_path}' does not exist.")
        return

    if os.path.isdir(target_path):
        process_directory(target_path)
    elif target_path.lower().endswith(".parquet"):
        process_parquet(target_path)
    else:
        print("Error: Target must be a directory or a .parquet file.")

if __name__ == "__main__":
    main()