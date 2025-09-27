import os
import shutil
import zipfile

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DATA_YEARS = range(2016, 2025)
DATA_URL_STUB = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/"


def fetch_traffic_data():
    """Fetch the traffic data from 2016-2024 if we don't have them already."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for year in DATA_YEARS:
        out_csv_file = f"{year}.csv"
        temp_dir = os.path.join(DATA_DIR, str(year))

        if os.path.exists(os.path.join(DATA_DIR, out_csv_file)):
            print(f"Already have {out_csv_file}, skipping...")
            continue

        # Download the zip file
        dataset_title = f"Unfallorte{year}_EPSG25832_CSV.zip"
        downloaded_zip_file_path = os.path.join(DATA_DIR, dataset_title)
        fetch_data(dataset_title, downloaded_zip_file_path)

        # Extract the zip file
        extract_zip(temp_dir, downloaded_zip_file_path)
        os.remove(downloaded_zip_file_path)

        # Move the relevant files to DATA_DIR
        move_files_to_data_dir(temp_dir, year)

        # Delete the whole temporary extraction directory
        print(f"Cleaning up temporary files for {year}...")
        shutil.rmtree(temp_dir)


def fetch_data(dataset_title: str, download_path: str):
    """Fetches the dataset from the URL if it doesn't already exist.

    Args:
        dataset_title (str): The title of the dataset to fetch.
        download_path (str): The path to save the downloaded file.
    """
    if not os.path.exists(download_path):
        url = f"{DATA_URL_STUB}{dataset_title}"
        print(f"Fetching {dataset_title}...")
        os.system(f"curl -o {download_path} {url}")
    else:
        print(f"Already have {dataset_title}, skipping...")


def extract_zip(extract_path: str, file_path: str):
    """Extracts the zip file to the specified path if it hasn't been extracted yet.
    Args:
        extract_path (str): The path to extract the zip file to.
        file_path (str): The path of the zip file to extract.
        data_title (str): The title of the dataset (for logging purposes).
    """
    file_name = os.path.basename(file_path)[:-4]
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)
        print(f"Extracting {file_name}...")

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)
    else:
        print(f"Already extracted {file_name}, skipping...")


def move_files_to_data_dir(extract_path: str, year: int):
    """Moves files ending in .txt or .csv from extract_path to DATA_DIR,
    renaming them to [year].csv.

    Args:
        extract_path (str): The path to search for files.
        year (int): The year to use in the new filename.
    """
    for subdir, _, files in os.walk(extract_path):
        for file in files:
            if file.endswith(".txt") or file.endswith(".csv"):
                os.rename(
                    os.path.join(subdir, file),
                    os.path.join(DATA_DIR, f"{year}.csv"),
                )


if __name__ == "__main__":
    fetch_traffic_data()
