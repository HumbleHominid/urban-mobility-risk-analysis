"""Streamlit dashboard: DBSCAN hotspots of accidents with personal injury in any
German district or municipality, with an overview of the area, a short summary for Germany and
the Frankfurt case study the project started with.

Run from the repo root:

    uv run streamlit run src/urban_mobility/dashboard.py

The accident data is downloaded from the Unfallatlas on the first run (progress bar,
about 100 MB, a few seconds) and then kept in memory for all sessions. The default
minimum accidents of an area are found on its first visit (progress bar, a few seconds
for big cities) and kept as well.

The videos come from manim.ipynb and ``python -m urban_mobility.manim``; tabs
show a hint instead when they have not been rendered.
"""

import math
import threading
import time

import matplotlib
import numpy as np
import pandas as pd
import streamlit as st

from urban_mobility import fetch_data as fd
from urban_mobility.clusters import (
    cluster_centers,
    dbscan_labels,
    location_outliers,
    tune_min_samples,
)
from urban_mobility.dashboard_data import extract
from urban_mobility.utils import load_config, resolve_path

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
SUBSETS = {
    "All accidents": lambda df: df,
    "Bicycle": lambda df: df[df["IstRad"] == 1],
    "Pedestrian": lambda df: df[df["IstFuss"] == 1],
    "Night": lambda df: df[df["ULICHTVERH"] == 2],
    "Serious or fatal": lambda df: df[df["UKATEGORIE"].isin([1, 2])],
}


@st.cache_resource
def shared() -> dict:
    """One per server process, shared by all sessions: the loaded data, the tuned
    minimum samples per area, and locks so each is computed once. (This runs
    outside st caches: they cannot replay the progress bar updates.)"""
    return {"lock": threading.Lock(), "tune_lock": threading.Lock()}


def load_data() -> pd.DataFrame:
    state = shared()
    if "data" not in state:
        text = "Loading the accident data from the Unfallatlas…"
        progress = st.progress(0.0, text=text)
        try:
            with state["lock"]:
                if "data" not in state:  # another session may have loaded it meanwhile
                    state["data"] = extract(
                        YEARS,
                        lambda done, total, year: progress.progress(
                            done / total, text=f"{text} {year} done ({done}/{total})"
                        ),
                    )
        finally:
            progress.empty()
    return state["data"]


@st.cache_data
def area_labels() -> dict[str, dict[str, str]]:
    """Area key -> name, by name, per picker: "City" holds the kreisfreie cities
    (5-digit key) and the municipalities with at least 50 accidents (8-digit key),
    "District" the other districts. A municipality sharing its name with another
    city gets its district added ("Münster (Darmstadt-Dieburg)")."""
    pop = fd.get_district_population().sort_values("UJAHR").drop_duplicates("district", keep="last")
    groups = {"City": {}, "District": {}}
    for key, name, kreisfrei in zip(pop["district"], pop["name"], pop["kreisfrei"]):
        groups["City" if kreisfrei else "District"][key] = name
    names = pop.set_index("district")["name"]
    counts = shared()["data"].groupby(["District_key", "UGEMEINDE"], observed=True).size()
    big = {f"{d}{g:03d}" for (d, g), n in counts.items() if n >= 50}
    mun = fd.get_municipality_population()
    # 61/62: kreisfreie cities, already districts; 65/66: unincorporated areas (no inhabitants)
    mun = mun[(mun["UJAHR"] == mun["UJAHR"].max()) & ~mun["kind"].isin([61, 62, 65, 66])]
    cities = groups["City"]
    cities |= {key: name for key, name in zip(mun["municipality"], mun["name"]) if key in big}
    shared_name = pd.Series(cities).duplicated(keep=False)
    for key in shared_name[shared_name].index:
        if len(key) == 8:
            cities[key] += f" ({names.get(key[:5], key[:5])})"
    return {g: dict(sorted(labels.items(), key=lambda item: item[1])) for g, labels in groups.items()}


def area_mask(area: str) -> pd.Series:
    """Mask of the raw accidents of a district (5-digit key) or municipality (8-digit)."""
    df = shared()["data"]
    mask = df["District_key"] == area[:5]
    return mask & (df["UGEMEINDE"] == int(area[5:])) if len(area) == 8 else mask


@st.cache_resource(max_entries=50)  # not copied per call like st.cache_data; callers don't mutate it
def area_accidents(area: str) -> pd.DataFrame:
    """The area's accidents without the location outliers."""
    df = shared()["data"][area_mask(area)]
    return df[~location_outliers(df)]


def n_location_outliers(area: str) -> int:
    return int(area_mask(area).sum()) - len(area_accidents(area))


def subset_accidents(area: str, subset: str, years: tuple[int, int]) -> pd.DataFrame:
    df = area_accidents(area)
    return SUBSETS[subset](df[df["UJAHR"].between(*years)])


def tuned_min_samples(area: str) -> dict[str, int]:
    """Default minimum samples per subset: the smallest leaving at most 5 hotspots over
    all years with ε = 100 m. Computed on the area's first visit, then kept."""
    state = shared()
    key = f"tuned_{area}"
    if key not in state:
        name = (area_labels()["City"] | area_labels()["District"])[area]
        text = f"Finding the hotspot settings for {name}…"
        progress = st.progress(0.0, text=text)
        try:
            with state["tune_lock"]:
                if key not in state:  # another session may have tuned it meanwhile
                    frames = {s: f(area_accidents(area)) for s, f in SUBSETS.items()}
                    sizes = sum(len(f) for f in frames.values())
                    work = {"done": 0, "start": time.time()}

                    def on_step(n: int):
                        def step(_, max_steps):
                            work["done"] += n
                            frac = min(work["done"] / (sizes * max_steps), 1.0)
                            left = (time.time() - work["start"]) * (1 - frac) / frac
                            progress.progress(frac, text=f"{text} about {left:.0f} s left")
                        return step

                    progress.progress(0.0, text=f"{text} estimating…")
                    state[key] = {
                        s: tune_min_samples(f, 100, on_step=on_step(len(f)))
                        for s, f in frames.items()
                    }
        finally:
            progress.empty()
    return state[key]


@st.cache_data
def area_person_years(area: str, years: tuple[int, ...]) -> pd.Series:
    """The area's population per year in ``years`` (years without a row are missing)."""
    pop, col = (
        (fd.get_district_population(), "district")
        if len(area) == 5
        else (fd.get_municipality_population(), "municipality")
    )
    pop = pop[(pop[col] == area) & pop["UJAHR"].isin(years)]
    return pop.set_index("UJAHR")["population"]


@st.cache_data
def load_germany() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Accidents per state and year (with the state's name and its population that
    year), and per year and severity."""
    counts = (
        shared()["data"]
        .groupby(["District_key", "UJAHR", "UKATEGORIE"], observed=True)
        .size()
        .rename("accidents")
        .reset_index()
    )
    counts["ULAND"] = counts["District_key"].astype(str).str[:2]  # state = first 2 digits
    per_state = (
        counts.groupby(["ULAND", "UJAHR"])["accidents"]
        .sum()
        .reset_index()
        .merge(fd.get_state_population(), on=["ULAND", "UJAHR"])
    )
    severity = counts.pivot_table(
        index="UJAHR", columns="UKATEGORIE", values="accidents", aggfunc="sum", fill_value=0
    )
    return per_state, severity


@st.cache_data(max_entries=32)
def clustered(
    area: str, subset: str, years: tuple[int, int], eps: int, min_samples: int
) -> pd.DataFrame:
    """Accidents of ``subset`` in ``years`` with their DBSCAN ``labels``."""
    df = subset_accidents(area, subset, years)
    labels = dbscan_labels(df, eps, min_samples) if len(df) else np.empty(0, dtype=int)
    return df.assign(labels=labels)


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


st.set_page_config(page_title="Germany Accident Hotspots", layout="wide")
st.title("Accidents with Personal Injury in Germany")
st.caption(
    f"Unfallatlas data, {YEARS[0]}–{YEARS[-1]}. Hotspots found with DBSCAN on the accident locations."
)

try:
    load_data()
    fd.get_municipality_population()
except Exception as e:  # a download failed: say so instead of a traceback
    st.error(f"Could not load the data ({e}). Please reload the page to try again.")
    st.stop()

groups = area_labels()
labels = groups["City"] | groups["District"]
with st.sidebar:
    st.header("Filters")
    left, right = st.columns([1, 2], vertical_alignment="center")
    kind = left.radio("Area", list(groups), label_visibility="collapsed")
    options = groups[kind]
    # each picker's choice, kept here: Streamlit drops a widget's state while it is hidden
    picked = st.session_state.setdefault(
        "picked", {"City": fd.get_city_key("Frankfurt am Main"), "District": next(iter(groups["District"]))}
    )
    if f"area_{kind}" not in st.session_state:
        st.session_state[f"area_{kind}"] = picked[kind]
    area = picked[kind] = right.selectbox(
        kind, list(options), format_func=options.get, key=f"area_{kind}", label_visibility="collapsed"
    )
tuned = tuned_min_samples(area)  # outside the sidebar: the progress bar needs the width
with st.sidebar:
    years = st.slider("Years", YEARS[0], YEARS[-1], (YEARS[0], YEARS[-1]))
    subset = st.selectbox("Accidents", list(SUBSETS))
    st.header("Settings")
    eps = st.slider("ε: neighbourhood radius (m)", 25, 300, 100, step=25)
    min_samples = st.number_input(
        "Minimum accidents",
        min_value=2,
        max_value=500,
        value=tuned[subset],
        key=f"min_samples_{area}_{subset}",
        help="Accidents within ε needed to start a hotspot. Defaults are the smallest number "
        "that leaves at most 5 hotspots over all years with ε = 100 m; lower it when you narrow the years.",
    )

tabs = st.tabs(
    ["Hotspots", "Overview", "Germany", "Frankfurt case study", "Acknowledgements"],
    key="tab",
    on_change="rerun",
)
hotspots, overview, germany, case_study, acknowledgements = tabs

with hotspots:
    if hotspots.open:
        df = clustered(area, subset, years, eps, int(min_samples))
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
        if len(points):
            # zoom so that the 1st-99th percentile extent fills the map (Frankfurt: 11.5)
            q = points[["XGCSWGS84", "YGCSWGS84"]].quantile([0.01, 0.99])
            extent = max((q.loc[0.99] - q.loc[0.01]).max(), 1e-3)
            st.map(
                points,
                latitude="YGCSWGS84",
                longitude="XGCSWGS84",
                color="color",
                size="size",
                zoom=min(max(math.log2(360 / extent) + 1, 5), 15),
            )
            st.caption(
                "Gray: accidents outside any hotspot (noise). Colors: one per hotspot, as in the table below. "
                f"Left out in all tabs but Germany: {n_location_outliers(area)} accidents of {YEARS[0]}–{YEARS[-1]} "
                "whose reported location is far from the rest of the area (fewer than 5 others within 5 km)."
            )

        if sizes.empty:
            st.warning("No hotspots: lower the minimum accidents or raise ε.")
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
                one["UTYP1"].value_counts(),
                horizontal=True,
                sort=False,  # value_counts order: largest first
                color=CLUSTER_COLORS[lab % 20],
            )
            c[1].markdown("**Hour of day**")
            c[1].bar_chart(
                one["USTUNDE"].value_counts().reindex(range(24), fill_value=0),
                color=CLUSTER_COLORS[lab % 20],
            )
            c[2].markdown("**Participants involved**")
            c[2].bar_chart(
                one[list(PARTICIPANTS)].sum().rename(PARTICIPANTS).sort_values(ascending=False),
                horizontal=True,
                sort=False,
                color=CLUSTER_COLORS[lab % 20],
            )

with overview:
    if overview.open:
        st.subheader(labels[area])
        local = area_accidents(area)
        local = local[local["UJAHR"].between(*years)]
        n_years = local["UJAHR"].nunique()
        per_state, _ = load_germany()
        # rows are state-years: accidents over the population summed over the years with
        # data, for Germany and the area alike (not every state reports from 2016)
        de_rate = (per_state["accidents"].sum() / per_state["population"].sum()) * 1000
        population = area_person_years(area, tuple(sorted(local["UJAHR"].unique())))
        # only years with accidents and a population row: else the rate is inflated
        local_rate = local["UJAHR"].isin(population.index).sum() / population.sum() * 1000 if len(population) else None
        c = st.columns(5)
        c[0].metric("Accidents", f"{len(local):,}")
        c[1].metric("Per year", f"{len(local) / max(n_years, 1):,.0f}")
        c[2].metric(
            "Per 1,000 inhabitants and year",
            f"{local_rate:.2f}" if local_rate is not None else "-",
            f"{local_rate - de_rate:+.2f} vs Germany" if local_rate is not None else None,
            delta_color="inverse",
            help="Germany: all reporting states and years.",
        )
        c[3].metric("Fatal", int((local["UKATEGORIE"] == 1).sum()))
        c[4].metric("Serious or fatal", share(local["UKATEGORIE"].isin([1, 2])))

        c = st.columns(2)
        c[0].markdown("**Accidents per year**")
        c[0].bar_chart(local["UJAHR"].value_counts().sort_index())
        c[1].markdown("**Participants involved (share of accidents)**")
        c[1].bar_chart(
            local[list(PARTICIPANTS)].mean().rename(PARTICIPANTS).sort_values(ascending=False),
            horizontal=True,
            sort=False,
        )
        c = st.columns(3)
        c[0].markdown("**Hour of day**")
        c[0].bar_chart(local["USTUNDE"].value_counts().reindex(range(24), fill_value=0))
        c[1].markdown("**Day of the week**")
        c[1].bar_chart(
            local["UWOCHENTAG"]
            .value_counts()
            .reindex(range(1, 8), fill_value=0)
            .set_axis(WEEKDAYS),
            sort=False,
        )
        c[2].markdown("**Accident type**")
        c[2].bar_chart(local["UTYP1"].value_counts(), horizontal=True, sort=False)

with germany:
    if germany.open:
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
        rates = per_state.groupby("state")[["accidents", "population"]].sum()
        c[1].markdown("**Accidents per 1,000 inhabitants and year, by state**")
        c[1].bar_chart(
            (
                rates["accidents"] / rates["population"] * 1000
            ).sort_values(ascending=False),
            horizontal=True,
            sort=False,
        )
        video(
            "PlotAccidents",
            f"A sample of the accidents of {YEARS[-1]}, month by month, colored by state.",
        )

with case_study:
    if case_study.open:
        st.subheader("Ten years of accidents")
        st.markdown(
            "We took a closer look at Frankfurt am Main for our 2025 Introduction to Data Analytics "
            "in Business course at Frankfurt School. The city has about 2,500 accidents with personal "
            "injury every year. On a map of all of them, the city centre and the major roads stand out, "
            "but it is hard to point at specific places, so we let DBSCAN find the hotspots."
        )
        video(
            "PlotFFM",
            f"All Frankfurt accidents, year by year ({YEARS[0]}–{YEARS[-1]}), colored by participant.",
        )
        st.subheader("The hotspots")
        st.markdown(
            "Four places came out on top: the area around the Hauptbahnhof, the A661, Eschenheimer Tor "
            "in the city centre and the crossing of Adickesallee and Eckenheimer Landstraße, right next "
            "to Frankfurt School. Different road users meet their risks in different places: the "
            "hotspots of cyclist accidents lie along the Main. This dashboard grew out of that work and "
            "runs the same analysis for every district in Germany."
        )
        video(
            "FFMClusters",
            "The hotspots of all accidents with the default parameters (ε = 100 m, min. accidents = 99).",
        )

with acknowledgements:
    if acknowledgements.open:
        # Same sources as the report (report/references.bib)
        st.markdown(
            """
**Data.** Accidents with personal injury from the
[Unfallatlas](https://unfallatlas.statistikportal.de/) of the Statistische Ämter des Bundes und
der Länder, downloaded from [OpenGeodata.NRW](https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/)
under the [Data licence Germany – attribution – version 2.0](https://www.govdata.de/dl-de/by-2-0).
Populations of the states and districts on 31 December of each year: Statistisches Bundesamt
(Destatis), GENESIS-Online, statistic [12411](https://genesis.destatis.de/datenbank/online/statistic/12411)
(tables [12411-0010](https://genesis.destatis.de/datenbank/online/statistic/12411/table/12411-0010) for the states and
[12411-0015](https://genesis.destatis.de/datenbank/online/statistic/12411/table/12411-0015) for the districts),
under the same licence.
Populations and names of the municipalities on 31 December of each year: Statistisches Bundesamt (Destatis),
[Gemeindeverzeichnis](https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/_inhalt.html),
annual editions GV100AD 2016–2025, reproduced with attribution as its terms allow.
All data were processed: aggregated, filtered and joined with each other.

**Maps.** Map data © [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors.

**Method.** Point pattern analysis following Rey, Arribas-Bel and Wolf,
[*Geographic Data Science with Python*: Point Pattern Analysis](https://geographicdata.science/book/notebooks/08_point_pattern_analysis.html).
Clustering with [scikit-learn](https://scikit-learn.org/)'s DBSCAN, animations made with
[Manim](https://www.manim.community/), dashboard built with [Streamlit](https://streamlit.io/).

**Authors.** Adrian Frings, Gaziza Janabayeva and Michael Fryer. The code is on
[GitHub](https://github.com/HumbleHominid/urban-mobility-risk-analysis).
ChatGPT (OpenAI), Gemini (Google) and Claude (Anthropic) assisted with the code.
"""
        )
