"""DBSCAN cluster analysis shared by the Frankfurt notebooks."""

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import DBSCAN

from urban_mobility import plotting as pl
from urban_mobility.utils import cached_df


def analyze_clusters(df, name, eps, min_samples, cluster_to_plot, breakdown=()):
    """DBSCAN on the metric coordinates, then print cluster centers, map the
    clusters and chart the accident types of one cluster.

    The DBSCAN labels are cached as an artifact keyed on ``name``, ``eps`` and
    ``min_samples`` (``name`` identifies the subset of accidents in ``df``).
    ``breakdown`` lists columns whose per-cluster value counts are printed.
    Returns the clustered (non-noise) rows, with a ``labels`` column.
    """
    if df.empty:
        print("Check Filter")
        return df
    df = df.copy()

    def compute() -> pd.DataFrame:
        labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(
            df[["LINREFX", "LINREFY"]]
        )
        return pd.DataFrame({"labels": labels})

    key = f"clusters/{name}_eps{eps}_min{min_samples}"
    labels = cached_df(key, compute)
    if len(labels) != len(df):  # cached for different data
        labels = cached_df(key, compute, force=True)
    df["labels"] = labels["labels"].to_numpy()
    clusters_df = df[df["labels"] != -1]
    print(f"Found {clusters_df['labels'].nunique()} clusters.")
    if clusters_df.empty:
        print("No clusters found (only noise). Map and charts will not be generated.")
        return clusters_df

    centers = clusters_df.groupby("labels")[["YGCSWGS84", "XGCSWGS84"]].mean()
    centers.columns = ["Latitude", "Longitude"]
    centers["Google_Maps_Link"] = (
        "https://www.google.com/maps/search/?api=1&query="
        + centers["Latitude"].astype(str) + "," + centers["Longitude"].astype(str)
    )
    pd.set_option("display.max_colwidth", None)
    print("Cluster Center Coordinates")
    print(centers)

    fig, ax = plt.subplots(figsize=(10, 10), dpi=144)
    pl.cluster_map(df, df["labels"], pad=0.1, ax=ax)
    plt.show()

    for col in breakdown:
        print(f"\n--- Breakdown of {col} by Cluster ---")
        print(clusters_df.groupby("labels")[col].value_counts())

    one = clusters_df[clusters_df["labels"] == cluster_to_plot]
    if one.empty:
        print(f"Error: Cluster {cluster_to_plot} not found or is empty.")
        print(f"Available clusters are: {list(clusters_df['labels'].unique())}")
        return clusters_df
    counts = one["UTYP1"].astype(str).value_counts()
    fig, ax = plt.subplots(figsize=(9, 4))
    pl.hbar(
        counts.index, counts.values,
        f"Accident Types (UTYP1) for Cluster {cluster_to_plot}",
        "Count of Accidents", "Accident Type (UTYP1)", ax=ax,
    )
    ax.set_xlim(0, counts.max() * 1.15)
    plt.show()
    return clusters_df
