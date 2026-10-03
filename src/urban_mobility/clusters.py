"""DBSCAN cluster analysis shared by the Frankfurt notebooks."""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors

from urban_mobility import plotting as pl
from urban_mobility.utils import cached_df


def knn_distances(df, k):
    """Distance of each point to its ``k``-th nearest neighbour (itself counts
    as the 1st) on the metric coordinates, for choosing DBSCAN ``eps``."""
    xy = df[["LINREFX", "LINREFY"]]
    distances, _ = NearestNeighbors(n_neighbors=k).fit(xy).kneighbors(xy)
    return distances[:, k - 1]


def dbscan_labels(df, eps, min_samples) -> np.ndarray:
    """DBSCAN cluster label per row (-1 = noise) on the metric coordinates."""
    return DBSCAN(eps=eps, min_samples=min_samples).fit_predict(df[["LINREFX", "LINREFY"]])


def cluster_centers(clusters_df) -> pd.DataFrame:
    """Mean location and a Google Maps link per cluster of the non-noise rows."""
    centers = clusters_df.groupby("labels")[["YGCSWGS84", "XGCSWGS84"]].mean()
    centers.columns = ["Latitude", "Longitude"]
    centers["Google_Maps_Link"] = (
        "https://www.google.com/maps/search/?api=1&query="
        + centers["Latitude"].astype(str) + "," + centers["Longitude"].astype(str)
    )
    return centers


def analyze_clusters(df, name, eps, min_samples, cluster_to_plot, save_as=None):
    """DBSCAN on the metric coordinates, then print cluster centers, map the
    clusters and chart the accident types of one cluster.

    The DBSCAN labels are cached as an artifact keyed on ``name``, ``eps`` and
    ``min_samples`` (``name`` identifies the subset of accidents in ``df``).
    ``save_as`` also writes the cluster map to <output.figures>/<save_as>.png.
    Returns the clustered (non-noise) rows, with a ``labels`` column.
    """
    if df.empty:
        print("Check Filter")
        return df
    df = df.copy()

    def compute() -> pd.DataFrame:
        return pd.DataFrame({"labels": dbscan_labels(df, eps, min_samples)})

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

    centers = cluster_centers(clusters_df)
    pd.set_option("display.max_colwidth", None)
    print("Cluster Center Coordinates")
    print(centers)

    fig, ax = plt.subplots(figsize=(10, 10), dpi=144)
    pl.cluster_map(df, df["labels"], pad=0.1, ax=ax)
    if save_as:
        pl.save(fig, save_as)
    plt.show()

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
