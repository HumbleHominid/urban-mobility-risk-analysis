"""Streamlit dashboard: DBSCAN hotspots of accidents with personal injury in
Frankfurt am Main, with a Frankfurt overview and a short summary for Germany.

Run from the repo root:

    uv run streamlit run src/urban_mobility/dashboard.py

The videos come from manim.ipynb and ``python -m urban_mobility.manim``; tabs
show a hint instead when they have not been rendered.
"""

import matplotlib
import pandas as pd
import streamlit as st

from urban_mobility import fetch_data as fd
from urban_mobility.clusters import cluster_centers, dbscan_labels, knn_distances
from urban_mobility.utils import load_config, resolve_path

CITY = "Frankfurt am Main"
YEARS = fd.DATA_YEARS
cfg = load_config()
VIDEOS = resolve_path(cfg.output.media) / "videos" / "720p30"
CLUSTER_COLORS = [
    matplotlib.colors.to_hex(c) for c in matplotlib.colormaps["tab20"].colors
]
WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]  # UWOCHENTAG 1=Sunday
PARTICIPANTS = {
    "IstRad": "Bicycle",
    "IstPKW": "Car",
    "IstFuss": "Pedestrian",
    "IstKrad": "Motorcycle",
    "IstGkfz": "Goods vehicle",
    "IstSonstige": "Other",
}
# Accident subsets and the min_samples chosen for them in frankfurt_clusters.ipynb (eps = 100 m)
SUBSETS = {
    "All accidents": (lambda df: df, 99),
    "Bicycle": (lambda df: df[df["IstRad"] == 1], 46),
    "Pedestrian": (lambda df: df[df["IstFuss"] == 1], 20),
    "Night": (lambda df: df[df["ULICHTVERH"] == 2], 27),
    "Serious or fatal": (lambda df: df[df["UKATEGORIE"].isin([1, 2])], 14),
}


@st.cache_data
def load_city() -> pd.DataFrame:
    return fd.get_city_accidents(CITY, YEARS, label_utyp1=True).reset_index(drop=True)


@st.cache_data
def city_population() -> int:
    info = fd.get_city_info()
    return int(info.loc[info["city"] == CITY, "population"].iat[0])


@st.cache_data
def load_germany() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Accidents per state and year (with the state's name and inhabitants), and
    per year and severity."""
    df = pd.concat(fd.get_dfs(YEARS).values(), ignore_index=True)
    states = pd.read_csv(resolve_path(f"{cfg.data.raw}/ULAND_Inhabitants.csv"), sep=";")
    states["ULAND"] = states["ULAND"].astype(str).str.zfill(2)
    per_state = (
        df.groupby(["ULAND", "UJAHR"])
        .size()
        .rename("accidents")
        .reset_index()
        .merge(states, on="ULAND")
    )
    severity = df.groupby(["UJAHR", "UKATEGORIE"]).size().unstack(fill_value=0)
    return per_state, severity


@st.cache_data
def clustered(
    subset: str, years: tuple[int, int], eps: int, min_samples: int
) -> pd.DataFrame:
    """Accidents of ``subset`` in ``years`` with their DBSCAN ``labels``."""
    df = load_city()
    df = SUBSETS[subset][0](df[df["UJAHR"].between(*years)])
    return df.assign(labels=dbscan_labels(df, eps, min_samples))


def video(name: str, caption: str) -> None:
    path = VIDEOS / f"{name}.mp4"
    if path.exists():
        st.video(str(path))
        st.caption(caption)
    else:
        st.info(
            f"Video `{name}` not rendered yet (see manim.ipynb / `python -m urban_mobility.manim`)."
        )


def share(mask) -> str:
    return f"{mask.mean():.1%}" if len(mask) else "-"


st.set_page_config(page_title="Frankfurt Accident Hotspots", layout="wide")
st.title("Accidents with Personal Injury in Frankfurt am Main")
st.caption(
    f"Unfallatlas data, {YEARS[0]}–{YEARS[-1]}. Hotspots found with DBSCAN on the accident locations."
)

with st.sidebar:
    st.header("Frankfurt filters")
    years = st.slider("Years", YEARS[0], YEARS[-1], (YEARS[0], YEARS[-1]))
    subset = st.selectbox("Accidents", list(SUBSETS))
    st.header("DBSCAN")
    eps = st.slider("ε: neighbourhood radius (m)", 25, 300, 100, step=25)
    min_samples = st.number_input(
        "Minimum samples",
        min_value=2,
        max_value=500,
        value=SUBSETS[subset][1],
        key=f"min_samples_{subset}",
        help="Defaults are tuned for all years and ε = 100 m to give at most 5 hotspots; "
        "lower it when you narrow the years.",
    )

hotspots, overview, germany, method = st.tabs(
    ["Hotspots", "Frankfurt overview", "Germany", "How DBSCAN works"]
)

with hotspots:
    df = clustered(subset, years, eps, int(min_samples))
    in_clusters = df[df["labels"] != -1]
    sizes = in_clusters["labels"].value_counts()
    c = st.columns(4)
    c[0].metric("Accidents", f"{len(df):,}")
    c[1].metric("Hotspots", len(sizes))
    c[2].metric("Accidents in hotspots", share(df["labels"] != -1))
    c[3].metric("Largest hotspot", f"{sizes.max() if len(sizes) else 0:,}")

    points = df[["YGCSWGS84", "XGCSWGS84"]].assign(
        color=[
            CLUSTER_COLORS[lab % 20] if lab != -1 else "#99999955"
            for lab in df["labels"]
        ],
        size=[25 if lab != -1 else 8 for lab in df["labels"]],
    )
    # zoom: Streamlit fits Frankfurt at 10; +log2(1.5) zooms in by about 50%
    st.map(
        points,
        latitude="YGCSWGS84",
        longitude="XGCSWGS84",
        color="color",
        size="size",
        zoom=11.5,
    )
    st.caption(
        "Gray: accidents outside any hotspot (noise). Colors: one per hotspot, as in the table below."
    )

    if sizes.empty:
        st.warning("No hotspots: lower the minimum samples or raise ε.")
    else:
        table = cluster_centers(in_clusters).join(
            in_clusters.groupby("labels").agg(
                Accidents=("labels", "size"),
                Serious_or_fatal=("UKATEGORIE", lambda s: s.isin([1, 2]).mean()),
                Night=("ULICHTVERH", lambda s: (s == 2).mean()),
                Pedestrian=("IstFuss", "mean"),
                Bicycle=("IstRad", "mean"),
                Most_common_type=("UTYP1", lambda s: s.value_counts().index[0]),
            )
        )
        table = table.sort_values("Accidents", ascending=False)
        pct = st.column_config.NumberColumn(format="percent")
        st.subheader("Hotspots")
        st.dataframe(
            table.style.apply(
                lambda row: [f"color: {CLUSTER_COLORS[row.name % 20]}"] * len(row),
                axis=1,
                subset=["Accidents"],
            ),
            column_config={
                "Google_Maps_Link": st.column_config.LinkColumn(
                    "Location", display_text="Google Maps"
                ),
                "Serious_or_fatal": st.column_config.NumberColumn(
                    "Serious or fatal", format="percent"
                ),
                "Night": pct,
                "Pedestrian": pct,
                "Bicycle": pct,
                "Most_common_type": "Most common type",
                "Latitude": st.column_config.NumberColumn(format="%.4f"),
                "Longitude": st.column_config.NumberColumn(format="%.4f"),
            },
        )

        st.subheader("Hotspot detail")
        lab = st.selectbox(
            "Hotspot",
            table.index,
            format_func=lambda i: f"Cluster {i} ({sizes[i]} accidents)",
        )
        one = in_clusters[in_clusters["labels"] == lab]
        c = st.columns(3)
        c[0].markdown("**Accident type**")
        c[0].bar_chart(
            one["UTYP1"].value_counts(), horizontal=True, color=CLUSTER_COLORS[lab % 20]
        )
        c[1].markdown("**Hour of day**")
        c[1].bar_chart(
            one["USTUNDE"].value_counts().reindex(range(24), fill_value=0),
            color=CLUSTER_COLORS[lab % 20],
        )
        c[2].markdown("**Participants involved**")
        c[2].bar_chart(
            one[list(PARTICIPANTS)].sum().rename(PARTICIPANTS),
            horizontal=True,
            color=CLUSTER_COLORS[lab % 20],
        )

    video(
        "FFMClusters",
        "The hotspots of all accidents with the default parameters (ε = 100 m, min. samples = 99).",
    )

with overview:
    ffm = load_city()
    ffm = ffm[ffm["UJAHR"].between(*years)]
    n_years = ffm["UJAHR"].nunique()
    per_state, _ = load_germany()
    # rows are state-years, so the summed inhabitants count each state once per year with data
    de_rate = (per_state["accidents"].sum() / (per_state["INHABITANTS"]).sum()) * 1000
    ffm_rate = len(ffm) / city_population() * 1000 / max(n_years, 1)
    c = st.columns(5)
    c[0].metric("Accidents", f"{len(ffm):,}")
    c[1].metric("Per year", f"{len(ffm) / max(n_years, 1):,.0f}")
    c[2].metric(
        "Per 1,000 inhabitants and year",
        f"{ffm_rate:.2f}",
        f"{ffm_rate - de_rate:+.2f} vs Germany",
        delta_color="inverse",
        help="Germany: all reporting states and years.",
    )
    c[3].metric("Fatal", int((ffm["UKATEGORIE"] == 1).sum()))
    c[4].metric("Serious or fatal", share(ffm["UKATEGORIE"].isin([1, 2])))

    c = st.columns(2)
    c[0].markdown("**Accidents per year**")
    c[0].bar_chart(ffm["UJAHR"].value_counts().sort_index())
    c[1].markdown("**Participants involved (share of accidents)**")
    c[1].bar_chart(
        ffm[list(PARTICIPANTS)].mean().rename(PARTICIPANTS).sort_values(),
        horizontal=True,
    )
    c = st.columns(3)
    c[0].markdown("**Hour of day**")
    c[0].bar_chart(ffm["USTUNDE"].value_counts().reindex(range(24), fill_value=0))
    c[1].markdown("**Day of the week**")
    c[1].bar_chart(
        ffm["UWOCHENTAG"]
        .value_counts()
        .reindex(range(1, 8), fill_value=0)
        .set_axis(WEEKDAYS),
        sort=False,
    )
    c[2].markdown("**Accident type**")
    c[2].bar_chart(ffm["UTYP1"].value_counts(), horizontal=True)
    video(
        "PlotFFM",
        f"All Frankfurt accidents, year by year ({YEARS[0]}–{YEARS[-1]}), colored by participant.",
    )

with germany:
    per_state, severity = load_germany()
    full = per_state.groupby("ULAND")["UJAHR"].transform("nunique").eq(len(YEARS))
    c = st.columns(4)
    c[0].metric("Accidents", f"{per_state['accidents'].sum():,}")
    c[1].metric("States reporting every year", per_state.loc[full, "ULAND"].nunique())
    c[2].metric("Fatal", f"{severity[1].sum():,}")
    c[3].metric(
        "Serious or fatal",
        f"{(severity[1].sum() + severity[2].sum()) / severity.sum().sum():.0%}",
    )
    st.caption(
        "Not every state reports from 2016 (all 16 only from 2020), so the yearly trend uses the states that report every year."
    )
    c = st.columns(2)
    c[0].markdown("**Accidents per year (states reporting every year)**")
    c[0].bar_chart(per_state[full].groupby("UJAHR")["accidents"].sum())
    rates = per_state.groupby("STATE").agg(
        accidents=("accidents", "sum"),
        years=("UJAHR", "nunique"),
        inhabitants=("INHABITANTS", "first"),
    )
    c[1].markdown("**Accidents per 1,000 inhabitants and year, by state**")
    c[1].bar_chart(
        (
            rates["accidents"] / rates["inhabitants"] * 1000 / rates["years"]
        ).sort_values(),
        horizontal=True,
    )
    video(
        "PlotAccidents",
        f"A sample of the accidents of {YEARS[-1]}, month by month, colored by state.",
    )

with method:
    st.markdown(
        "DBSCAN groups points that lie densely together and leaves isolated points as noise. "
        "A point with at least *minimum samples* neighbours within the radius **ε** is a *core point*; "
        "core points within ε of each other form a cluster, together with the points they reach. "
        "It finds hotspots of any shape (an intersection, a stretch of road) and does not force every accident into a cluster. "
        "We fix ε = 100 m and raise the minimum samples until at most five hotspots remain."
    )
    video("DBSCANExplainer", "Core points, border points and noise.")
    st.markdown(
        f"**k-distance graph** for the selected accidents (k = minimum samples = {int(min_samples)}): "
        "the distance of each accident to its k-th nearest neighbour, sorted. Accidents below the ε line "
        "are core points."
    )
    df = clustered(subset, years, eps, int(min_samples))
    if len(df) > min_samples:
        dist = pd.DataFrame(
            {
                "k-distance (m)": sorted(knn_distances(df, int(min_samples))),
                "ε (m)": float(eps),
            }
        )
        st.line_chart(dist, x_label="Accidents, sorted", y_label="Distance (m)")
