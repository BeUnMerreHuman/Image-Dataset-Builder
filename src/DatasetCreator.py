import os
import io
import pandas as pd
from pathlib import Path
from PIL import Image, UnidentifiedImageError
from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")
CSV_PATH = 'metadata.csv'
DATASET_DIR_NAME = os.getenv("DATASET_DIR", "dataset")


LOCAL_PARQUET_PATH = Path(f"{DATASET_DIR_NAME}.parquet")
MASTER_PARQUET_PATH = Path(__file__).parent.parent / f"{DATASET_DIR_NAME}.parquet"

GREY_COLOR = (128, 128, 128)

def process_image(img_path):
    """
    Reads image, handles transparency, pads to square, returns bytes.
    """
    with Image.open(img_path) as img:
        has_alpha = img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info)
        
        if has_alpha:
            img = img.convert("RGBA")
            background = Image.new("RGB", img.size, GREY_COLOR)
            background.paste(img, mask=img.split()[3])
            img = background
        else:
            img = img.convert("RGB")

        width, height = img.size
        if width != height:
            max_dim = max(width, height)
            square_img = Image.new("RGB", (max_dim, max_dim), GREY_COLOR)
            
            x = (max_dim - width) // 2
            y = (max_dim - height) // 2
            
            square_img.paste(img, (x, y))
            img = square_img

        buffer = io.BytesIO()
        img.save(buffer, format='JPEG')
        return buffer.getvalue()

def build_dataset(csv_path, output_path, master_path):
    existing_df = pd.DataFrame()
    existing_ids = set()

    if os.path.exists(master_path):
        try:
            existing_df = pd.read_parquet(master_path)
            if 'id' in existing_df.columns:
                existing_ids = set(existing_df['id'].astype(str))
        except Exception:
            pass

    if not os.path.exists(csv_path):
        print("No CSV file found. Aborting.")
        return

    df = pd.read_csv(csv_path)

    new_rows = []

    for index, row in df.iterrows():
        img_path = str(row['image_path'])
        
        if pd.isna(img_path) or not os.path.exists(img_path):
            continue
        img_id = Path(img_path).stem
        
        if img_id in existing_ids:
            continue
        
        if any(d['id'] == img_id for d in new_rows):
            continue

        try:
            img_bytes = process_image(img_path)
            
            new_rows.append({
                'image': img_bytes,
                'id': img_id,
                'label': row['label'],
                'url': row['image_url'],
                'timestamp': row['timestamp']
            })
            
        except (UnidentifiedImageError, OSError, IOError):
            continue

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        
        if not existing_df.empty:        
            missing_cols = set(existing_df.columns) - set(new_df.columns)
            for col in missing_cols:
                new_df[col] = None 
            
            new_df = new_df[existing_df.columns.intersection(new_df.columns)]
            
            try:
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                print(f"Concatenation failed: {e}")
                return
        else:
            final_df = new_df

        try:
            final_df.to_parquet(output_path, engine='pyarrow', index=False)
        except Exception as e:
            print(f"Error saving parquet file: {e}")
    
    else:
        if not existing_df.empty:
            existing_df.to_parquet(output_path, engine='pyarrow', index=False)

if __name__ == "__main__":
    build_dataset(CSV_PATH, LOCAL_PARQUET_PATH, MASTER_PARQUET_PATH)