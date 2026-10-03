import hashlib
import shutil
import urllib.request
import zipfile

import pandas as pd

from urban_mobility.utils import cached_df, load_config, resolve_path

DATA_DIR = resolve_path(load_config().data.raw)
DATA_YEARS = range(2016, 2026)
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
    """Fetch the traffic data for DATA_YEARS if we don't have them already."""
    for year in DATA_YEARS:
        fetch_year(year)


def fetch_year(year: int) -> None:
    """Download the data of ``year`` to <data.raw>/<year>.csv unless it is there."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = DATA_DIR / f"{year}.csv"
    if out_csv.exists():
        print(f"Already have {out_csv.name}, skipping...")
        return

    title = f"Unfallorte{year}_EPSG25832_CSV.zip"
    zip_path = DATA_DIR / title
    extract_dir = DATA_DIR / str(year)

    print(f"Fetching {title}...")
    urllib.request.urlretrieve(f"{DATA_URL_STUB}{title}", zip_path)
    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(extract_dir)
        # Keep the extracted data file, named [year].csv
        src = next(p for p in extract_dir.rglob("*") if p.suffix in (".txt", ".csv"))
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

    # Zero-pad the codes (2021 drops the leading zeros of ULAND and UGEMEINDE)
    for col, width in {"ULAND": 2, "UREGBEZ": 1, "UKREIS": 2, "UGEMEINDE": 3}.items():
        df[col] = df[col].str.zfill(width)

    # District (Kreis) key, as in the population table. The city-states Berlin and
    # Hamburg are one district each (their data codes boroughs in UREGBEZ/UKREIS)
    df["District_key"] = df["ULAND"] + df["UREGBEZ"] + df["UKREIS"]
    df.loc[df["ULAND"].isin(["11", "02"]), "District_key"] = df["ULAND"] + "000"

    # We drop columns for identifiers that we don't care about for analysis
    df.drop(
        # Object ids differ per year (OID_, OBJECTID, OBJECTID_1; none from 2025)
        columns=[
            "UIDENTSTLAE",
            "UIDENTSTLA",
            "FID",
            "PLST",
            "OID_",
            "OBJECTID",
            "OBJECTID_1",
        ],
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
        },
        inplace=True,
    )

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
    df = df[df["District_key"] == get_city_key(city)]
    if label_utyp1:
        df = df.assign(UTYP1=df["UTYP1"].map(UTYP1_LABELS))
    return df


def get_district_aggregate(
    years: list[int] | range, aggs: dict[str, str]
) -> pd.DataFrame:
    """Aggregate accidents per district and year and join the district's population
    of that year (``get_district_population``).

    One-hot encodes injury severity (``inj_light/serious/fatal``) and lighting
    (``daylight/twilight/darkness``) first, so ``aggs`` may sum those columns. Adds
    ``inj_total``. District-years without a population are dropped (Eisenach in 2021,
    after it merged into the Wartburgkreis: 0.05% of that year's accidents). Cached as
    an artifact keyed on the arguments, so changing ``years`` or ``aggs`` rebuilds it
    (set ``run.force_recompute`` after other changes).
    """
    if type(years) is range:
        years = list(years)

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
        grouped = df.groupby(["District_key", "UJAHR"]).agg(aggs).reset_index()
        grouped = grouped.rename(columns={"District_key": "district"})
        merged = grouped.merge(get_district_population(), on=["district", "UJAHR"], how="inner")
        merged["inj_total"] = (
            merged["inj_light"] + merged["inj_serious"] + merged["inj_fatal"]
        )
        return merged

    key = hashlib.md5(repr((years, sorted(aggs.items()))).encode()).hexdigest()[:8]
    return cached_df(f"district_aggregate/{key}", build)


def _read_population(file: str) -> pd.DataFrame:
    """``code``, ``label``, ``UJAHR``, ``population`` from a GENESIS-Online flat file
    (CSV, "ffcsv") of Destatis statistic 12411: the population on 31 December."""
    df = pd.read_csv(DATA_DIR / file, sep=";", encoding="utf-8-sig", dtype=str)
    df = df[df["value"] != "-"]  # "-": the district did not exist that year
    return pd.DataFrame(
        {
            "code": df["1_variable_attribute_code"],
            "label": df["1_variable_attribute_label"],
            "UJAHR": df["time"].str[:4].astype(int),
            "population": df["value"].astype(int),
        }
    )


def get_state_population() -> pd.DataFrame:
    """Population per state (``ULAND``, ``state``) and year, from table 12411-0010."""
    return _read_population("population_land.csv").rename(columns={"code": "ULAND", "label": "state"})


def get_district_population() -> pd.DataFrame:
    """Population per district (``district`` key, ``name``) and year, from table
    12411-0015. ``kreisfrei``
    marks the independent cities (kreisfreie Städte), the only cities with a row."""
    df = _read_population("population_stadt.csv").rename(columns={"code": "district"})
    label = df.pop("label").str.replace(r" \(until .*\)$", "", regex=True)
    df["kreisfrei"] = label.str.endswith(", kreisfreie Stadt")
    df["name"] = label.str.replace(r", (kreisfreie Stadt|Landkreis)$", "", regex=True)
    return df


def get_city_key(city: str) -> str:
    """District key of the independent city ``city`` (e.g. "Frankfurt am Main")."""
    df = get_district_population()
    return df.loc[df["kreisfrei"] & (df["name"] == city), "district"].iat[0]


if __name__ == "__main__":
    fetch_traffic_data()
