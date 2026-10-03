"""Manim scenes for the dashboard: how DBSCAN works, and Frankfurt's hotspots.

Needs the manim dependency group. Render from the repo root with

    uv run --group manim python -m urban_mobility.manim

(run it as a module: as a script this file would shadow the ``manim`` package).
Videos go to <output.media>/videos/720p30/<Scene>.mp4, where the dashboard finds them.
"""

import matplotlib
import numpy as np
from manim import (
    DOWN,
    GRAY,
    LEFT,
    RIGHT,
    UP,
    WHITE,
    YELLOW,
    Axes,
    Circle,
    Create,
    Dot,
    FadeIn,
    FadeOut,
    Indicate,
    Scene,
    Text,
    Title,
    VGroup,
    config,
)
from sklearn.cluster import DBSCAN

from urban_mobility import fetch_data as fd
from urban_mobility.clusters import dbscan_labels
from urban_mobility.utils import load_config, resolve_path

# Same colors as plotting.cluster_map and the dashboard
CLUSTER_COLORS = [matplotlib.colors.to_hex(c) for c in matplotlib.colormaps["tab20"].colors]
# DBSCAN parameters for all accidents, as chosen in frankfurt_clusters.ipynb
EPS, MIN_SAMPLES = 100, 99


class DBSCANExplainer(Scene):
    """Core, border and noise points on synthetic data, then the clusters."""

    def construct(self):
        eps, min_samples = 0.6, 5
        rng = np.random.default_rng(1)
        pts = np.vstack(
            [
                rng.normal([-2.5, 0.3], 0.45, (40, 2)),
                rng.normal([2.2, -0.4], 0.55, (40, 2)),
                rng.uniform([-5.5, -2.6], [5.5, 2.4], (18, 2)),
            ]
        )
        db = DBSCAN(eps=eps, min_samples=min_samples).fit(pts)
        labels = db.labels_
        core = np.zeros(len(pts), bool)
        core[db.core_sample_indices_] = True
        border = (labels != -1) & ~core
        dots = VGroup(*[Dot([x, y - 0.4, 0], radius=0.06, color=WHITE) for x, y in pts])

        title = Title("How DBSCAN finds clusters", include_underline=False, font_size=40)
        caption = Text("", font_size=26).to_edge(DOWN)
        self.play(FadeIn(title), FadeIn(dots, lag_ratio=0.02), run_time=2)

        def explain(i, text, color):
            nonlocal caption
            ring = Circle(radius=eps, color=color).move_to(dots[i])
            new_caption = Text(text, font_size=26).to_edge(DOWN)
            self.play(Create(ring), FadeOut(caption), FadeIn(new_caption))
            caption = new_caption
            self.play(dots[i].animate.set_color(color).scale(1.6), run_time=0.8)
            self.wait(2)
            self.play(FadeOut(ring), dots[i].animate.scale(1 / 1.6))

        explain(
            int(np.flatnonzero(core)[0]),
            f"Core point: at least {min_samples} points within distance ε",
            YELLOW,
        )
        if border.any():
            explain(
                int(np.flatnonzero(border)[0]),
                "Border point: fewer neighbours, but within ε of a core point",
                "#9ecae1",
            )
        explain(
            int(np.flatnonzero(labels == -1)[0]),
            "Noise: neither, so it belongs to no cluster",
            GRAY,
        )

        new_caption = Text("Clusters: core points linked through their neighbours", font_size=26).to_edge(DOWN)
        self.play(
            FadeOut(caption),
            FadeIn(new_caption),
            *[
                dot.animate.set_color(GRAY if lab == -1 else CLUSTER_COLORS[lab % 20]).set_opacity(0.4 if lab == -1 else 1)
                for dot, lab in zip(dots, labels)
            ],
            run_time=2,
        )
        self.wait(4)


class FFMClusters(Scene):
    """All Frankfurt accidents in gray, then the DBSCAN hotspots one by one."""

    def construct(self):
        df = fd.get_city_accidents("Frankfurt am Main", label_utyp1=True)
        labels = dbscan_labels(df, EPS, MIN_SAMPLES)

        x_min, x_max = df["LINREFX"].min(), df["LINREFX"].max()
        y_min, y_max = df["LINREFY"].min(), df["LINREFY"].max()
        axes = Axes(x_range=[x_min, x_max], y_range=[y_min, y_max], x_length=x_max - x_min, y_length=y_max - y_min)
        axes.scale_to_fit_height(config.frame_height * 0.78).to_edge(LEFT).shift(DOWN * 0.3)

        def dot(row, radius=0.012, **kw):
            return Dot(axes.c2p(row.LINREFX, row.LINREFY), radius=radius, **kw)

        title = Text(  # Text, not Title: LaTeX cannot typeset the ε
            f"Accident hotspots in Frankfurt (DBSCAN, ε = {EPS} m, min. samples = {MIN_SAMPLES})",
            font_size=26,
        ).to_edge(UP)
        background = VGroup(*[dot(row, color=GRAY, fill_opacity=0.35) for row in df.itertuples()])
        self.play(FadeIn(title))
        self.play(FadeIn(background), run_time=2)
        self.wait()

        legend = VGroup()
        clusters = df.assign(labels=labels)[labels != -1]
        sizes = clusters["labels"].value_counts()  # largest first
        for lab, n in sizes.items():
            color = CLUSTER_COLORS[lab % 20]
            members = clusters[clusters["labels"] == lab]
            group = VGroup(*[dot(row, color=color, radius=0.03) for row in members.itertuples()])
            center = axes.c2p(members["LINREFX"].mean(), members["LINREFY"].mean())
            ring = Circle(radius=0.35, color=color).move_to(center)
            top_type = members["UTYP1"].value_counts().index[0]
            line = VGroup(
                Text(f"Cluster {lab}: {n} accidents", font_size=22, color=color),
                Text(f"mostly {top_type.lower()}", font_size=16, color=WHITE),
            ).arrange(DOWN, aligned_edge=LEFT, buff=0.08)
            legend.add(line)
            legend.arrange(DOWN, aligned_edge=LEFT, buff=0.3).to_edge(RIGHT).shift(UP * 0.3)
            self.play(FadeIn(group), Create(ring), FadeIn(line))
            self.play(Indicate(group, color=color, scale_factor=1.2))
            self.play(FadeOut(ring))
            self.wait(0.5)

        share = len(clusters) / len(df)
        summary = Text(
            f"{len(sizes)} hotspots hold {share:.1%} of {len(df):,} accidents ({fd.DATA_YEARS[0]}-{fd.DATA_YEARS[-1]})",
            font_size=22,
        ).to_edge(DOWN)
        self.play(FadeIn(summary))
        self.wait(5)


if __name__ == "__main__":
    config.media_dir = str(resolve_path(load_config().output.media))
    config.video_dir = "{media_dir}/videos/{quality}"
    config.quality = "medium_quality"
    config.disable_caching = True  # hashing ~25k dots is slower than rendering them
    for scene in (DBSCANExplainer, FFMClusters):
        scene().render()
