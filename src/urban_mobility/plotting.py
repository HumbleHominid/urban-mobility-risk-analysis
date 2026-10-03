"""Shared matplotlib/seaborn theme and charts for the notebooks.

Importing this module applies the project theme. Every chart function takes an
optional ``ax`` (so charts can be placed in ``plt.subplots`` grids), draws on it
and returns it. Use ``save`` to write a figure to ``output.figures`` (see configs/config.yaml).
"""

import contextily as cx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter, PercentFormatter

from urban_mobility.utils import load_config, resolve_path

COLOR = "#494373"
SEQUENTIAL = "ch:s=.25,rot=-.25"
QUALITATIVE = "tab20"
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]  # UWOCHENTAG 1=Sunday

# Human-readable labels; order matters (first 4 = injury categories, last 4 = types)
MEASURE_LABELS = {
    # injury category
    "inj_total": "Total Injuries",
    "inj_light": "Light Injuries",
    "inj_serious": "Serious Injuries",
    "inj_fatal": "Fatal Injuries",
    # participant type
    "IstFuss": "Pedestrian Accidents",
    "IstRad": "Cyclist Accidents",
    "IstKrad": "Motorcycle Accidents",
    "IstGkfz": "Delivery Vehicle Accidents",
}
MEASURE_GROUPS = {
    "categories": list(MEASURE_LABELS.items())[:4],
    "types": list(MEASURE_LABELS.items())[4:],
}

# (lat, lon, color, label) of Frankfurt's centres
FFM_CENTERS = [
    (50.121250, 8.636583, "purple", "Geographical Center"),
    (50.117306, 8.644417, "red", "Physical Center"),
]
BASEMAP = cx.providers.Esri.WorldGrayCanvas  # CartoDB tiles now need an API key


# Project theme, applied on import
sns.set_theme(
    context="notebook",
    style="whitegrid",
    palette=[COLOR],
    rc={
        "figure.figsize": (10, 5),
        "figure.dpi": 100,
        "axes.titlesize": 16,
        "axes.titleweight": "bold",
        "axes.labelsize": 13,
        "axes.spines.top": False,
        "axes.spines.right": False,
    },
)


thousands = FuncFormatter(lambda v, _: f"{v:,.0f}")


def mean_centers(df, specs, lat="YGCSWGS84", lon="XGCSWGS84"):
    """``(lat, lon, color, label)`` of the mean location of the rows where
    ``df[col] == 1``, for each ``(col, color, label)`` in ``specs``."""
    out = []
    for col, color, label in specs:
        sub = df[df[col] == 1]
        out.append((sub[lat].mean(), sub[lon].mean(), color, f"Mean Center ({label})"))
    return out


def _ax(ax):
    return ax if ax is not None else plt.subplots()[1]


def _label(ax, title=None, xlabel=None, ylabel=None):
    if title is not None:
        ax.set_title(title)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    return ax


def save(fig, name: str, dpi=300, **kwargs) -> None:
    """Write ``fig`` to <output.figures>/<name>.png."""
    path = resolve_path(load_config().output.figures) / f"{name}.png"
    path.parent.mkdir(exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight", **kwargs)


def bar_counts(
    counts: pd.Series,
    title=None,
    xlabel=None,
    ylabel="Count",
    labels=None,
    gradient=False,
    annotate=True,
    ax=None,
):
    """Vertical bar per category from a Series of counts indexed by category
    (e.g. ``df["UMONAT"].value_counts()``). ``labels`` maps category -> text."""
    ax = _ax(ax)
    counts = counts.sort_index()
    x = [labels.get(i, i) for i in counts.index] if labels else list(counts.index)
    kw = (
        dict(hue=x, palette=sns.color_palette(SEQUENTIAL, len(x)), legend=False)
        if gradient
        else dict(color=COLOR)
    )
    sns.barplot(x=x, y=counts.values, ax=ax, **kw)
    if annotate:
        ax.bar_label(ax.containers[0], fmt="{:,.0f}", fontsize=9, padding=2)
    ax.yaxis.set_major_formatter(thousands)
    return _label(ax, title, xlabel, ylabel)


def hbar(labels, values, title=None, xlabel=None, ylabel=None, fmt="{:,.0f}", ax=None):
    """Horizontal bar with value labels; first item is drawn on top."""
    ax = _ax(ax)
    sns.barplot(x=list(values), y=list(labels), orient="h", color=COLOR, ax=ax)
    ax.bar_label(ax.containers[0], fmt=fmt, padding=3)
    return _label(ax, title, xlabel, ylabel)


def stacked_bar(
    df: pd.DataFrame,
    title=None,
    xlabel=None,
    ylabel=None,
    pct=False,
    label_fn=None,
    ax=None,
):
    """Stacked bar: one bar per index row, one segment per column.

    ``label_fn`` maps a segment's height to its centered label ("" hides it).
    """
    ax = _ax(ax)
    if pct:
        df = df.div(df.sum(axis=1), axis=0) * 100
    df.plot.bar(
        stacked=True,
        ax=ax,
        rot=0,
        width=0.8,
        color=sns.color_palette(SEQUENTIAL, len(df.columns))[::-1],
    )
    if label_fn:
        for c in ax.containers:
            ax.bar_label(
                c,
                labels=[label_fn(v.get_height()) for v in c],
                label_type="center",
                color="white",
                fontsize=9,
                fontweight="bold",
            )
    ax.yaxis.set_major_formatter(PercentFormatter() if pct else thousands)
    ax.grid(False)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), frameon=False)
    return _label(ax, title, xlabel, ylabel)


def line_chart(df, x, y, title=None, xlabel=None, ylabel=None, hue=None, ax=None):
    """Line with markers; ``hue`` splits into one line per group. ``x`` is
    plotted as a category (years)."""
    ax = _ax(ax)
    data = df.assign(**{x: df[x].astype(str)})
    sns.lineplot(
        data=data,
        x=x,
        y=y,
        hue=hue,
        marker="o",
        ax=ax,
        **({"palette": QUALITATIVE} if hue else {"color": COLOR}),
    )
    if hue:
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), frameon=False, title=hue)
    return _label(ax, title, xlabel, ylabel)


def lorenz(x, y, gini, title=None, val_title="value", ax=None):
    """Lorenz curve (cumulative population share ``x`` vs value share ``y``)
    with the line of equality and the Gini index."""
    ax = _ax(ax)
    ax.plot(x, y, color=COLOR, marker=".", markersize=3)
    ax.plot([0, 1], [0, 1], color="red", linestyle="--")
    ax.text(
        0.95,
        0.05,
        f"Gini Index: {gini:.3f}",
        transform=ax.transAxes,
        ha="right",
        fontweight="bold",
    )
    ax.set_aspect("equal")
    return _label(ax, title, "Proportion of Population", f"Proportion of {val_title}")


def scatter_fit(df, x, y, title=None, xlabel=None, ylabel=None, ax=None):
    """Scatter with a dashed least-squares line and r² label."""
    ax = _ax(ax)
    sns.scatterplot(data=df, x=x, y=y, alpha=0.3, color=COLOR, edgecolor=None, ax=ax)
    slope, intercept = np.polyfit(df[x], df[y], 1)
    r2 = np.corrcoef(df[x], df[y])[0, 1] ** 2
    xs = np.array([0, df[x].max() * 1.2])
    ax.plot(xs, slope * xs + intercept, color="red", linestyle="--")
    ax.set_xlim(0, df[x].max() * 1.05)
    ax.set_ylim(0, df[y].max() * 1.05)
    ax.text(0.02, 0.95, f"r² = {r2:.2f}", transform=ax.transAxes, va="top")
    ax.xaxis.set_major_formatter(thousands)
    ax.yaxis.set_major_formatter(thousands)
    return _label(ax, title, xlabel, ylabel)


def knn_distance(distances, k, title=None, ax=None):
    """Sorted k-th neighbour distances, for choosing DBSCAN eps."""
    ax = _ax(ax)
    ax.plot(np.sort(np.asarray(distances)), color=COLOR)
    return _label(
        ax,
        title or f"{k}-nearest-neighbour distance",
        "Points sorted by distance",
        f"Distance to {k}th neighbour",
    )


def _to_gdf(df, lat, lon) -> gpd.GeoDataFrame:
    """Lat/lon DataFrame -> GeoDataFrame in web mercator (what basemaps use)."""
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs="EPSG:4326"
    ).to_crs(epsg=3857)


def _finish_map(ax, title, basemap=True):
    ax.set_axis_off()
    if basemap:
        cx.add_basemap(ax, source=BASEMAP, attribution_size=6)
    if title:
        ax.set_title(title)
    return ax


def point_map(
    df,
    title=None,
    lat="YGCSWGS84",
    lon="XGCSWGS84",
    color="gray",
    size=2,
    alpha=0.6,
    centers=(),
    basemap=True,
    ax=None,
):
    """Accident points on a basemap with optional marker centers.

    ``centers`` is an iterable of ``(lat, lon, color, label)``.
    """
    ax = _ax(ax)
    _to_gdf(df, lat, lon).plot(ax=ax, markersize=size, color=color, alpha=alpha)
    for c_lat, c_lon, c_color, label in centers:
        _to_gdf(pd.DataFrame({"lat": [c_lat], "lon": [c_lon]}), "lat", "lon").plot(
            ax=ax, marker="x", color=c_color, markersize=100, linewidths=2, label=label
        )
    if centers:
        ax.legend(loc="upper right")
    return _finish_map(ax, title, basemap)


def silhouette(
    df,
    lat="YGCSWGS84",
    lon="XGCSWGS84",
    color="black",
    size=0.1,
    alpha=0.2,
    by=None,
    ax=None,
):
    """Axis-free points with no basemap (transparent when saved with
    ``transparent=True``); ``by`` colors points by a column."""
    ax = _ax(ax)
    _to_gdf(df, lat, lon).plot(
        ax=ax,
        markersize=size,
        alpha=alpha,
        **({"column": by} if by else {"color": color}),
    )
    return _finish_map(ax, None, basemap=False)


def cluster_map(
    df,
    labels,
    title=None,
    lat="YGCSWGS84",
    lon="XGCSWGS84",
    pad=None,
    basemap=True,
    ax=None,
):
    """Map of DBSCAN clusters: noise (-1) in gray, one color per cluster.

    ``pad`` (e.g. 0.1) zooms to a square around the clusters with that margin."""
    ax = _ax(ax)
    labels = np.asarray(labels)
    gdf = _to_gdf(df, lat, lon)
    gdf[labels == -1].plot(ax=ax, markersize=2, color="lightgray", alpha=0.5)
    cmap = plt.get_cmap(QUALITATIVE)
    for lab in sorted(set(labels) - {-1}):
        gdf[labels == lab].plot(
            ax=ax, markersize=6, color=cmap(lab % cmap.N), label=f"Cluster {lab}"
        )
    if pad is not None and (labels != -1).any():
        minx, miny, maxx, maxy = gdf[labels != -1].total_bounds
        half = max(maxx - minx, maxy - miny, 1) * (1 + 2 * pad) / 2
        ax.set_xlim((minx + maxx) / 2 - half, (minx + maxx) / 2 + half)
        ax.set_ylim((miny + maxy) / 2 - half, (miny + maxy) / 2 + half)
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), frameon=False)
    return _finish_map(ax, title, basemap)


if __name__ == "__main__":
    rng = np.random.default_rng(0)
    pts = pd.DataFrame(
        {
            "XGCSWGS84": rng.normal(8.65, 0.03, 200),
            "YGCSWGS84": rng.normal(50.11, 0.02, 200),
        }
    )
    counts = pd.Series(rng.integers(0, 24, 500)).value_counts()
    steps = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    yearly = pd.DataFrame({"x": [1, 2, 3], "y": [3, 4, 6]})

    fig, axes = plt.subplots(3, 4, figsize=(24, 15))
    a = iter(axes.flat)
    bar_counts(counts, "bar", ax=next(a))
    bar_counts(counts, "bar gradient", gradient=True, ax=next(a))
    hbar(["a", "b"], [3, 5], "hbar", ax=next(a))
    stacked_bar(steps, "stacked", pct=True, ax=next(a))
    line_chart(yearly, "x", "y", "line", ax=next(a))
    lorenz([0, 0.5, 1], [0, 0.2, 1], 0.3, "lorenz", "v", ax=next(a))
    scatter_fit(yearly, "x", "y", "fit", ax=next(a))
    knn_distance(rng.random(50), 5, ax=next(a))
    silhouette(pts, ax=next(a))
    point_map(
        pts, "points", centers=[(50.12, 8.64, "red", "c")], basemap=False, ax=next(a)
    )
    cluster_map(pts, rng.integers(-1, 3, 200), "clusters", basemap=False, ax=next(a))
    save(fig, "_smoke", dpi=50)
    (resolve_path(load_config().output.figures) / "_smoke.png").unlink()
    plt.close(fig)
    print("ok")
