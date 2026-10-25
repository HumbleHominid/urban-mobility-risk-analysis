"""The data the dashboard needs, loaded straight from the Unfallatlas.

The years are downloaded in parallel (skipped when <data.raw> already has the CSV),
then read one at a time and cut down to the dashboard's columns with small integer
types. Memory so holds all ~2.4M accidents of Germany in roughly 120 MB.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pandas.api.types import union_categoricals

from urban_mobility import fetch_data as fd

UTYP1_TYPE = pd.CategoricalDtype(list(fd.UTYP1_LABELS.values()))
# Columns of the accidents the dashboard uses
ACCIDENT_COLUMNS = [
    "UJAHR", "USTUNDE", "UWOCHENTAG", "UKATEGORIE", "ULICHTVERH", "UTYP1",
    "IstRad", "IstPKW", "IstFuss", "IstKrad", "IstGkfz", "IstSonstige",
    "LINREFX", "LINREFY", "XGCSWGS84", "YGCSWGS84", "District_key",
]
COORDINATES = ["LINREFX", "LINREFY", "XGCSWGS84", "YGCSWGS84"]


def extract(
    years=fd.DATA_YEARS, on_year: Callable[[int, int, int], None] | None = None
) -> pd.DataFrame:
    """All accidents of Germany (``UTYP1`` labelled, ``District_key`` and ``UTYP1``
    categorical, integer columns downcast), sorted by year.
    ``on_year(done, total, year)`` reports progress."""
    parts, keys = [], []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fd.fetch_year, year): year for year in years}
        for done, future in enumerate(as_completed(futures), 1):
            year = futures[future]
            future.result()  # re-raise a failed download
            # reindex: some years lack a column (no IstGkfz in 2017)
            df = fd._read_df(year).reindex(columns=ACCIDENT_COLUMNS)
            keys.append(df.pop("District_key").astype("category"))
            df["UTYP1"] = df["UTYP1"].map(fd.UTYP1_LABELS).astype(UTYP1_TYPE)
            for col in df.columns.difference(COORDINATES + ["UTYP1"]):
                df[col] = pd.to_numeric(df[col], downcast="integer")
                if df[col].dtype == "float64":
                    df[col] = df[col].astype("float32")
            parts.append(df)
            if on_year:
                on_year(done, len(years), year)
    df = pd.concat(parts, ignore_index=True)
    # per-year categories differ, so combine them apart: concat would make them object
    df["District_key"] = union_categoricals(keys)
    return df.sort_values("UJAHR", kind="stable").reset_index(drop=True)
