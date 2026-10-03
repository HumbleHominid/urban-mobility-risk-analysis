import hashlib
import shutil
import urllib.request
import zipfile

import pandas as pd

from urban_mobility.utils import cached_df, load_config, resolve_path

DATA_DIR = resolve_path(load_config().data.raw)
DATA_YEARS = range(2016, 2025)
DATA_URL_STUB = "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/"

UTYP1_LABELS = {
    1: "Driving accident",
    2: "Accident caused by turning off the road",
    3: "Accident caused by turning into a road or by crossing it",
    4: "Accident caused by crossing the road",
    5: "Accident involving stationary",
    6: "Accident between vehicles moving along in carriageway",
    7: "Other accident",
}


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
    """Cleaned accidents of ``year``, cached as an artifact."""
    assert (
        year in DATA_YEARS
    ), f"Year {year} not in available data years {list(DATA_YEARS)}"
    return cached_df(f"accidents/{year}", lambda: _read_df(year))


def _read_df(year: int) -> pd.DataFrame:
    """Read and clean the raw CSV for ``year``."""
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

    # City-states (Berlin, Hamburg) are one municipality each
    df.loc[df["ULAND"].isin(["11", "02"]), "Community_key"] = df["ULAND"] + "000000"

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
    df["UID"] = f"{year}_" + df["OID_"].astype(str)

    return df


def get_dfs(years: list[int]) -> dict[int, pd.DataFrame]:
    """``{year: get_df(year)}`` for each of ``years``."""
    return {year: get_df(year) for year in years}


def get_city_accidents(
    city: str = "Frankfurt am Main",
    years=DATA_YEARS,
    label_utyp1: bool = False,
) -> pd.DataFrame:
    """All accidents of ``city`` in ``years`` (rows in year order, original per-year
    index kept). ``label_utyp1`` replaces the ``UTYP1`` codes with ``UTYP1_LABELS``."""
    df = pd.concat(get_dfs(years).values())
    df = df[df["Community_key"] == get_regional_key(get_city_info(), city)]
    if label_utyp1:
        df = df.assign(UTYP1=df["UTYP1"].map(UTYP1_LABELS))
    return df


def get_city_aggregate(
    years: list[int], by: list[str], aggs: dict[str, str]
) -> pd.DataFrame:
    """Aggregate accidents per ``by`` columns and join the result with the city info.

    One-hot encodes injury severity (``inj_light/serious/fatal``) and lighting
    (``daylight/twilight/darkness``) first, so ``aggs`` may sum those columns. Adds
    ``inj_total``. Cached as an artifact keyed on the arguments, so changing ``years``,
    ``by`` or ``aggs`` rebuilds it (set ``run.force_recompute`` after other changes).
    """

    def build() -> pd.DataFrame:
        df = pd.concat(get_dfs(years).values(), ignore_index=True)
        df = pd.get_dummies(df, columns=["UKATEGORIE"], prefix="inj", dtype=int)
        df = pd.get_dummies(df, columns=["ULICHTVERH"], prefix="lum", dtype=int)
        df = df.rename(
            columns={
                "inj_3": "inj_light",
                "inj_2": "inj_serious",
                "inj_1": "inj_fatal",
                "lum_0": "daylight",
                "lum_1": "twilight",
                "lum_2": "darkness",
            }
        )
        grouped = df.groupby(by).agg(aggs).reset_index()
        grouped = grouped.rename(columns={"Community_key": "regional key"})
        merged = grouped.merge(get_city_info(), on="regional key", how="inner")
        merged["inj_total"] = (
            merged["inj_light"] + merged["inj_serious"] + merged["inj_fatal"]
        )
        return merged

    key = hashlib.md5(repr((years, by, sorted(aggs.items()))).encode()).hexdigest()[:8]
    return cached_df(f"city_aggregate/{key}", build)


def get_city_info() -> pd.DataFrame:
    """City area, population and regional key, from ``city_info.csv``."""
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
    """Regional key of ``city_name`` in the city info ``df``."""
    return df.loc[df["city"] == city_name, "regional key"].iat[0]


if __name__ == "__main__":
    fetch_traffic_data()
