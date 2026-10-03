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

A Streamlit dashboard shows the DBSCAN accident hotspots in Frankfurt (with adjustable
subset, years, ε and minimum samples), a Frankfurt overview and a short summary for Germany:

```sh
uv run streamlit run src/urban_mobility/dashboard.py
```

It embeds the rendered animations when they exist. The two made for it (how DBSCAN works,
and the Frankfurt hotspots) are rendered with `uv run --group manim python -m urban_mobility.manim`.
