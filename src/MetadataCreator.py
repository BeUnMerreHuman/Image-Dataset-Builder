import os
import csv
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")  

def match_images_to_csv(image_dir, csv_input, output_csv):
    
    print(f"Scanning image directory: {image_dir}")
    
    image_map = {}
    for root, _, files in os.walk(image_dir):
        for file in files:
            filepath = Path(root) / file
            character_name = filepath.parent.name
            image_map[file] = {
                'character': character_name,
                'image_path': str(filepath)
            }
            
    print(f"Found {len(image_map)} images on disk.")

    seen_filenames = set()  

    if os.path.exists(output_csv):
        try:
            with open(output_csv, mode='r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_path = row.get('image_path', '')
                    if existing_path:
                        filename = Path(existing_path).name
                        seen_filenames.add(filename)
            print(f"Loaded {len(seen_filenames)} existing records from {output_csv}.")
        except Exception as e:
            print(f"Warning: Could not read existing {output_csv}: {e}")

    new_matched_records = []

    try:
        with open(csv_input, mode='r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            
            if not reader.fieldnames:
                print(f"Warning: {csv_input} appears to be empty or malformed. Skipping.")
            else:
                for row_num, row in enumerate(reader, start=1):
                    try:
                        raw_image_path = row.get('image_path', '')
                        if not raw_image_path:
                            continue
                            
                        filename = raw_image_path.replace('\\', '/').split('/')[-1]

                        if filename in seen_filenames:
                            continue
                        if filename in image_map:
                            resolved_path = image_map[filename]['image_path']

                            seen_filenames.add(filename)
                            new_matched_records.append({
                                'character': image_map[filename]['character'],
                                'image_path': resolved_path,
                                'image_url': row.get('image_url', ''),
                                'timestamp': row.get('timestamp', '')
                            })
                    except Exception as row_error:
                        print(f"Skipping malformed row {row_num} in {csv_input}: {row_error}")
                        
    except Exception as file_error:
        print(f"Failed to read CSV {csv_input}: {file_error}")

    if not new_matched_records:
        print("No new matches found. metadata.csv is already up to date!")
        return

    print(f"Found {len(new_matched_records)} new matches. Appending to {output_csv}...")
    
    file_exists_and_not_empty = os.path.exists(output_csv) and os.path.getsize(output_csv) > 0

    try:
        with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
            fieldnames = ['character', 'image_path', 'image_url', 'timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists_and_not_empty:
                writer.writeheader()
                
            writer.writerows(new_matched_records)
        print("Done!")
    except Exception as e:
        print(f"Error writing output file: {e}")


if __name__ == "__main__":
    
    image_dir_path = Path(os.getenv("DATASET_DIR", "images")) 
    csv_input_path = "downloads.csv"
    output_csv = "metadata.csv"
    
    match_images_to_csv(image_dir_path, csv_input_path, output_csv)