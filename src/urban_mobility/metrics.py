"""Inequality metrics for the Gini / Lorenz notebooks."""

import numpy as np
import pandas as pd

from urban_mobility import fetch_data as fd


def lorenz_points(df: pd.DataFrame, pop_label: str, val_label: str):
    """Cumulative population share and value share, ordered by population and
    starting at (0, 0)."""
    s = df.sort_values(pop_label, ascending=True)
    pop = s[pop_label].cumsum() / s[pop_label].sum()
    val = s[val_label].cumsum() / s[val_label].sum()
    return np.insert(pop.values, 0, 0), np.insert(val.values, 0, 0)


def calc_gini(df: pd.DataFrame, pop_label: str, val_label: str) -> float:
    """Gini index of ``val_label`` over rows ordered by ``pop_label``.

    Can be negative: lower-population rows contribute more than their share.
    Based on @lucasboettcher - GitLab - https://gitlab.com/ComputationalScience/overdose-da/-/blob/main/county_plot/county_plots.ipynb
    """
    x, y = lorenz_points(df, pop_label, val_label)
    return 1 - 2 * np.trapezoid(x=x, y=y)


def gini_by_state(df: pd.DataFrame, val_label: str, pop_label: str = "population") -> pd.DataFrame:
    """Gini index of ``val_label`` per state and year (columns Year, Land, Gini) from a
    per-district aggregate with ``UJAHR`` and ``ULAND`` columns. State-years without
    data are left out, and so are states with fewer than 3 districts (Berlin and Hamburg
    are one district, Bremen two), where the index says nothing."""
    states = fd.get_state_population().drop_duplicates("ULAND").set_index("ULAND")["state"]
    return pd.DataFrame(
        [
            {"Year": year, "Land": states[land], "Gini": calc_gini(g, pop_label, val_label)}
            for (year, land), g in df.groupby(["UJAHR", "ULAND"])
            if len(g) >= 3
        ]
    )
