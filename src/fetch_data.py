import os
import shutil
import zipfile

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DATA_YEARS = range(2016, 2025)
DATA_URL_STUB = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/"


def fetch_traffic_data():
    """Fetch the traffic data from 2016-2024 if we don't have them already."""

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


def get_df(year: int) -> pd.DataFrame:
    """Get the dataframe for the specified year.
    Args:
        year (int): The year to get the dataframe for.
    Returns:
        pd.DataFrame: The dataframe for the specified year.
    """
    assert (
        year in DATA_YEARS
    ), f"Year {year} not in available data years {list(DATA_YEARS)}"

    path = os.path.join(DATA_DIR, f"{year}.csv")
    df = pd.read_csv(  # type: ignore
        path,
        sep=";",
        decimal=",",
        dtype={
            "UIDENTSTLAE": str,
            "UIDENTSTLA": str,
            "UGEMEINDE": str,
            "ULAND": str,
            "UREGBEZ": str,
            "UKREIS": str,
        },
    )

    # Create a community key column. This is how we can identify cities
    df["Community_key"] = df["ULAND"] + df["UREGBEZ"] + df["UKREIS"] + df["UGEMEINDE"]

    states = ["11", "02"]
    for s in states:
        df.loc[df["ULAND"] == s, "Community_key"] = f"{s}000000"

    # We drop columns for identifiers that we don't care about for analysis
    if "UIDENTSTLAE" in df.columns:
        df.drop("UIDENTSTLAE", axis=1, inplace=True)
    elif "UIDENTSTLA" in df.columns:
        df.drop("UIDENTSTLA", axis=1, inplace=True)

    if "FID" in df.columns:
        df.drop("FID", axis=1, inplace=True)

    if "PLST" in df.columns:
        df.drop("PLST", axis=1, inplace=True)

    # Rename columns to have consistent naming across years
    df.rename(
        columns={
            # Accident with other
            "IstSonstig": "IstSonstige",
            # Road Surface Condition
            "STRZUSTAND": "USTRZUSTAND",
            "IstStrasse": "USTRZUSTAND",
            "IstStrassenzustand": "USTRZUSTAND",
            # Light Condition
            "LICHT": "ULICHTVERH",
            # IDs
            "OBJECTID": "OID_",
            "OBJECTID_1": "OID_",
        },
        inplace=True,
    )

    # Create a unique id for the entry based on year and OID_
    df["UID"] = df["OID_"].apply(lambda x: f"{year}_{x}")  # type: ignore

    return df


def get_dfs(years: list[int]) -> dict[int, pd.DataFrame]:
    """Get the dataframes for the specified years.
    Args:
        years (list[int]): The years to get the dataframes for.

    Returns:
        dict[int, pd.DataFrame]: A dictionary mapping years to their dataframes.
    """
    assert all(
        year in DATA_YEARS for year in years
    ), f"Some years not in available data years {list(DATA_YEARS)}"
    return {year: get_df(year) for year in years}


def get_city_info() -> pd.DataFrame:
    """Fetches the city info from disk.

    Returns:
        pd.DataFrame: The city info as a Pandas dataframe
    """
    path = os.path.join(DATA_DIR, "city_info.csv")
    df = pd.read_csv(  # type: ignore
        path,
        sep=";",
        dtype={
            "city": str,
            "area in km²": float,
            "population": int,
        },
        converters={"regional key": lambda x: str(x)[:5] + str(x)[9:]},
    )
    df.rename(columns={"area in km²": "sq km"}, inplace=True)

    return df


def get_regional_key(df: pd.DataFrame, city_name: str) -> str:
    """Fetches the regional key for the specified city from a dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing city info.
        city_name (str): The name of the city to get the regional key for.

    Returns:
        str: The regional key for the specified city.
    """
    city_info = df[df["city"] == city_name]
    return city_info["regional key"].values[0]


if __name__ == "__main__":
    fetch_traffic_data()
    df = get_df(2024)
    city_info = get_city_info()
    berlin_key = get_regional_key(city_info, "Berlin")
    print(df[df["Community_key"] == berlin_key].head())
