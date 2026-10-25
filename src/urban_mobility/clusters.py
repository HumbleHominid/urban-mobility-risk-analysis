"""DBSCAN cluster analysis shared by the Frankfurt notebooks."""

import math

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


def location_outliers(df, k=5, radius=5000) -> np.ndarray:
    """True for points with fewer than ``k`` others within ``radius`` m: within one
    district, accidents whose reported location lies far from all the others (one
    Köln accident is placed near Jena). Checked against the district boundaries
    (BKG VG250), these thresholds catch 88% of the accidents more than 5 km outside
    their district and drop 0.004% of the rest, isolated rural accidents."""
    if len(df) <= k:
        return np.zeros(len(df), dtype=bool)
    return knn_distances(df, k + 1) > radius


def dbscan_labels(df, eps, min_samples) -> np.ndarray:
    """DBSCAN cluster label per row (-1 = noise) on the metric coordinates."""
    return DBSCAN(eps=eps, min_samples=min_samples).fit_predict(df[["LINREFX", "LINREFY"]])


def tune_min_samples(df, eps, max_hotspots=5, lo=5, hi=500, on_step=None) -> int:
    """Smallest ``min_samples`` in [``lo``, ``hi``] leaving at most ``max_hotspots``
    DBSCAN clusters (noise excluded), by binary search. The cluster count is only
    nearly monotonic in ``min_samples``, so this finds a boundary, not a global
    minimum. ``on_step(step, max_steps)`` follows each DBSCAN run (``max_steps``
    is an upper bound)."""
    if len(df) < lo:
        return lo
    step, max_steps = 0, math.ceil(math.log2(hi - lo + 1))
    while lo < hi:
        mid = (lo + hi) // 2
        if len(set(dbscan_labels(df, eps, mid)) - {-1}) <= max_hotspots:
            hi = mid
        else:
            lo = mid + 1
        step += 1
        if on_step:
            on_step(step, max_steps)
    return lo


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


if __name__ == "__main__":
    rng = np.random.default_rng(0)
    blobs = []
    for i in range(8):  # 10, 20, ..., 80 points within 5 m, blobs 10 km apart
        n = 10 * (i + 1)
        r, a = 5 * np.sqrt(rng.random(n)), 2 * np.pi * rng.random(n)
        blobs.append(pd.DataFrame({"LINREFX": 10_000 * i + r * np.cos(a), "LINREFY": r * np.sin(a)}))
    # blobs of >= 31 points: 40..80 = 5 clusters; at 30 there are 6
    assert tune_min_samples(pd.concat(blobs), 100) == 31
    assert tune_min_samples(pd.DataFrame({"LINREFX": [], "LINREFY": []}), 100) == 5
    # a lone point 50 km away is the only outlier; the smallest blob (10 points) is not
    lone = pd.DataFrame({"LINREFX": [0.0], "LINREFY": [50_000.0]})
    assert location_outliers(pd.concat([*blobs, lone])).tolist() == [False] * 360 + [True]
    print("ok")
