from __future__ import annotations

import functools
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from omegaconf import DictConfig, OmegaConf

if TYPE_CHECKING:
    import pandas as pd


def repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("pyproject.toml not found in any parent directory")


def resolve_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root() / path


@functools.cache
def _read_config() -> DictConfig:
    cfg = OmegaConf.load(repo_root() / "configs" / "config.yaml")
    assert isinstance(cfg, DictConfig)
    return cfg


def load_config(force_recompute: bool = False) -> DictConfig:
    """Load ``configs/config.yaml`` once; ``force_recompute`` re-reads it from disk."""
    if force_recompute:
        _read_config.cache_clear()
    return _read_config()


def cached[T](
    node: str | Path,
    compute: Callable[[], T],
    load: Callable[[Path], T],
    save: Callable[[T, Path], None],
    *,
    force: bool = False,
) -> T:
    """Load ``node`` if it exists, else compute it, save it, return it.

    ``force`` recomputes for this call only, which is what lets a single section
    be rebuilt without invalidating everything else.
    """
    path = resolve_path(node)

    if path.exists() and not force:
        return load(path)

    obj = compute()
    path.parent.mkdir(parents=True, exist_ok=True)
    save(obj, path)
    return obj


def cached_df(
    name: str, compute: Callable[[], pd.DataFrame], *, force: bool | None = None
) -> pd.DataFrame:
    """:func:`cached` for a DataFrame, stored as ``<output.artifacts>/<name>.parquet``.

    ``force`` defaults to ``run.force_recompute`` from the config.
    """
    import pandas as pd

    cfg = load_config()
    return cached(
        f"{cfg.output.artifacts}/{name}.parquet",
        compute,
        pd.read_parquet,
        lambda df, path: df.to_parquet(path),
        force=cfg.run.force_recompute if force is None else force,
    )
