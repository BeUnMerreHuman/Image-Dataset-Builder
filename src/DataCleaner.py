import os
import shutil
from pathlib import Path
from PIL import Image
import imagehash
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

HAMMING_THRESHOLD = int(os.getenv("HAMMING_THRESHOLD", 12))
MIN_WIDTH = int(os.getenv("MIN_WIDTH", 250))
MIN_HEIGHT = int(os.getenv("MIN_HEIGHT", 250))
VALID_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

TRASH_FOLDER = Path.cwd() / "Trash"


def process_image(img_path):
    try:
        with Image.open(img_path) as img:
            width, height = img.size
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                img = img.convert('RGBA')
                bg = Image.new('RGBA', img.size, (255, 255, 255, 255))
                img = Image.alpha_composite(bg, img)
            img = img.convert('RGB')
            return width, height, imagehash.phash(img)
    except Exception:
        return None, None, None


def move_to_trash(img_path):
    TRASH_FOLDER.mkdir(exist_ok=True)
    dest = TRASH_FOLDER / img_path.name
    if not dest.exists():
        shutil.move(str(img_path), dest)


def main():
    target_folder = Path(os.getenv("BASE_DOWNLOAD_DIR", "images"))
    if not target_folder.exists() or not target_folder.is_dir():
        print("Invalid directory.")
        return

    image_paths = [
        f for f in target_folder.rglob('*')
        if f.is_file() and f.suffix.lower() in VALID_EXTENSIONS
    ]
    if not image_paths:
        print("No images found.")
        return

    print(f"Scanning {len(image_paths)} images...")

    low_res_to_delete = []
    duplicates_to_delete = []
    unique_images = []

    for img_path in image_paths:
        width, height, phash = process_image(img_path)
        if not phash:
            continue

        if width < MIN_WIDTH or height < MIN_HEIGHT:
            low_res_to_delete.append(img_path)
            continue

        is_duplicate = False
        for i, (unique_path, unique_hash) in enumerate(unique_images):
            if phash - unique_hash <= HAMMING_THRESHOLD:
                if img_path.stat().st_size > unique_path.stat().st_size:
                    to_keep, to_delete = img_path, unique_path
                    unique_images[i] = (img_path, phash)
                else:
                    to_keep, to_delete = unique_path, img_path

                duplicates_to_delete.append((to_delete, to_keep))
                is_duplicate = True
                break

        if not is_duplicate:
            unique_images.append((img_path, phash))

    total_to_delete = len(low_res_to_delete) + len(duplicates_to_delete)

    print(f"\nAnalysis complete.")
    print(f"Low-resolution images (< {MIN_WIDTH}x{MIN_HEIGHT}): {len(low_res_to_delete)}")
    print(f"Near-duplicates found: {len(duplicates_to_delete)}")
    print(f"Total images slated for trash: {total_to_delete}")

    if total_to_delete == 0:
        print("Nothing to move. Exiting.")
        return

    moved_count = 0

    for img_path in low_res_to_delete:
        try:
            move_to_trash(img_path)
            moved_count += 1
        except Exception as e:
            print(f"  Could not move {img_path.name}: {e}")

    for img_path, _ in duplicates_to_delete:
        try:
            if img_path.exists():
                move_to_trash(img_path)
                moved_count += 1
        except Exception as e:
            print(f"  Could not move {img_path.name}: {e}")

    print(f"\nSuccessfully moved {moved_count} file(s) to: {TRASH_FOLDER}")


if __name__ == "__main__":
    main()