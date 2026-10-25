# Urban Mobility Risk Analysis

We seek to analyze traffic accidents in Frankfurt and attempt to answer the following questions:

- How do factors like time of day and weather influence the occurrence and severity of road accidents in Frankfurt?
  - Can we predict an uptick in traffic accidents in certain locations based on time of day and weather conditions?
- How to reduce the risk of urban travel in Frankfurt with respect to road accidents?
- Are certain types of road accidents more likely to occur depending on time and location?

## Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```sh
uv sync        # create .venv from uv.lock
```

The code in `src/urban_mobility` is installed into `.venv` (editable), so the notebooks in
`notebooks/` import it with `from urban_mobility import ...`. Select `.venv` as the notebook kernel (`ipykernel` is included), or run scripts with
`uv run python -m urban_mobility.fetch_data`.

The `manim` animations (`notebooks/manim.ipynb`) are optional and need system libraries
(macOS: `brew install cairo pango pkg-config`), then `uv sync --group manim`.

## Dashboard

A Streamlit dashboard shows the DBSCAN accident hotspots of any German district (pick it in
the sidebar; type to search), with adjustable subset, years, ε and minimum accidents per hotspot, an overview
of the district, a short summary for Germany and a Frankfurt case study tab:

```sh
uv run streamlit run src/urban_mobility/dashboard.py
```

It downloads the accident data from the Unfallatlas on its first run (with a progress bar,
about 100 MB, 6 s on a fast connection; instant locally once `data/raw` has the CSVs) and keeps it in memory.

Some accidents are reported with a location far outside their district (one Köln accident lies
near Jena). Within the selected district, accidents with fewer than 5 others within 5 km are left
out (`clusters.location_outliers`; checked against the BKG district boundaries, this catches 88% of
the accidents more than 5 km outside their district and drops 0.004% of the rest).

The default minimum accidents are tuned per district and subset (the smallest value that leaves at
most 5 hotspots over all years with ε = 100 m, found by binary search). This runs the first time a
district is opened, with a progress bar (a few seconds for big cities), and the result is kept.
Tabs only render while they are open, which keeps the dashboard responsive. The Frankfurt case
study tab shows the Frankfurt animations.

It embeds the rendered animations when they exist. The one made for it (the Frankfurt
hotspots) is rendered with `uv run --group manim python -m urban_mobility.manim`.
