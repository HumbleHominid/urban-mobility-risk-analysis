from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from omegaconf import DictConfig, OmegaConf

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "cached",
    "cached_df",
    "load_config",
    "load_dotenv",
    "repo_root",
    "resolve_path",
]

_CONFIG: DictConfig | None = None


def repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("pyproject.toml not found in any parent directory")


def resolve_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root() / path


def load_config(
    *overrides: str | DictConfig, force_recompute: bool = False
) -> DictConfig:
    global _CONFIG
    if _CONFIG is not None and not overrides and not force_recompute:
        return _CONFIG

    cfg = OmegaConf.load(repo_root() / "configs" / "config.yaml")
    for override in overrides:
        other = (
            OmegaConf.from_dotlist([override])
            if isinstance(override, str)
            else override
        )
        cfg = OmegaConf.merge(cfg, other)

    assert isinstance(cfg, DictConfig)
    _CONFIG = cfg
    return cfg


def load_dotenv(path: str | Path = ".env") -> bool:
    dotenv_path = resolve_path(path)
    if dotenv_path.exists():
        from dotenv import load_dotenv

        return load_dotenv(dotenv_path)
    return False


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
