import pandas as pd
import os
import shutil
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

csv_file = 'selected_images.csv'  
source_root = './'          
destination_root = Path(os.getenv("DATASET_DIR", "dataset"))

def organize_dataset():
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_file}")
        return

    if not os.path.exists(destination_root):
        os.makedirs(destination_root)
        print(f"Created main folder: {destination_root}")
    for index, row in df.iterrows():
        img_path = row['image_path']
        character = row['character']
        
        current_img_location = os.path.join(source_root, img_path)
        character_folder = os.path.join(destination_root, character)
        new_img_location = os.path.join(character_folder, os.path.basename(img_path))

        if not os.path.exists(character_folder):
            os.makedirs(character_folder)

        if os.path.exists(current_img_location):
            shutil.move(current_img_location, new_img_location)
            print(f"Moved: {img_path} -> {new_img_location}")
        else:
            print(f"Warning: File not found at {current_img_location}")

    print("\nOrganization complete!")

if __name__ == "__main__":
    organize_dataset()