import shutil
import urllib.request
import zipfile

import pandas as pd

from utils import resolve_path

DATA_DIR = resolve_path("src/data")
DATA_YEARS = range(2016, 2025)
DATA_URL_STUB = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/"


def fetch_traffic_data():
    """Fetch the traffic data from 2016-2024 if we don't have them already."""
    DATA_DIR.mkdir(exist_ok=True)

    for year in DATA_YEARS:
        out_csv = DATA_DIR / f"{year}.csv"
        if out_csv.exists():
            print(f"Already have {out_csv.name}, skipping...")
            continue

        title = f"Unfallorte{year}_EPSG25832_CSV.zip"
        zip_path = DATA_DIR / title
        extract_dir = DATA_DIR / str(year)

        print(f"Fetching {title}...")
        urllib.request.urlretrieve(f"{DATA_URL_STUB}{title}", zip_path)
        try:
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(extract_dir)
            # Keep the extracted data file, named [year].csv
            src = next(
                p for p in extract_dir.rglob("*") if p.suffix in (".txt", ".csv")
            )
            src.rename(out_csv)
        finally:
            zip_path.unlink(missing_ok=True)
            shutil.rmtree(extract_dir, ignore_errors=True)


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
    df = pd.read_csv(  # type: ignore
        DATA_DIR / f"{year}.csv",
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
    df.drop(
        columns=["UIDENTSTLAE", "UIDENTSTLA", "FID", "PLST"],
        errors="ignore",
        inplace=True,
    )

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
    return {year: get_df(year) for year in years}


def get_city_info() -> pd.DataFrame:
    """Fetches the city info from disk.

    Returns:
        pd.DataFrame: The city info as a Pandas dataframe
    """
    df = pd.read_csv(  # type: ignore
        DATA_DIR / "city_info.csv",
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
