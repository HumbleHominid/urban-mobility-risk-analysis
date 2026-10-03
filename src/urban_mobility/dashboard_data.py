"""The data the dashboard needs, loaded straight from the Unfallatlas.

The years are downloaded in parallel (skipped when <data.raw> already has the CSV),
then read one at a time, keeping only the city's accidents and the accident counts
per state. Memory so holds one year at a time instead of all ~2.4M accidents.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from urban_mobility import fetch_data as fd

CITY = "Frankfurt am Main"
# Columns of the city's accidents the dashboard uses
CITY_COLUMNS = [
    "UJAHR", "USTUNDE", "UWOCHENTAG", "UKATEGORIE", "ULICHTVERH", "UTYP1",
    "IstRad", "IstPKW", "IstFuss", "IstKrad", "IstGkfz", "IstSonstige",
    "LINREFX", "LINREFY", "XGCSWGS84", "YGCSWGS84",
]


def extract(
    years=fd.DATA_YEARS, on_year: Callable[[int, int, int], None] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The city's accidents (``UTYP1`` labelled) and the accidents per state, year
    and severity (``UKATEGORIE``). ``on_year(done, total, year)`` reports progress."""
    key = fd.get_regional_key(fd.get_city_info(), CITY)
    cities, counts = [], []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fd.fetch_year, year): year for year in years}
        for done, future in enumerate(as_completed(futures), 1):
            year = futures[future]
            future.result()  # re-raise a failed download
            df = fd._read_df(year)
            # reindex: some years lack a column (no IstGkfz in 2017)
            cities.append(df.loc[df["Community_key"] == key].reindex(columns=CITY_COLUMNS))
            counts.append(df.groupby(["ULAND", "UJAHR", "UKATEGORIE"]).size().rename("accidents").reset_index())
            if on_year:
                on_year(done, len(years), year)
    city = pd.concat(cities).sort_values("UJAHR", kind="stable").reset_index(drop=True)
    city["UTYP1"] = city["UTYP1"].map(fd.UTYP1_LABELS)
    return city, pd.concat(counts, ignore_index=True)
