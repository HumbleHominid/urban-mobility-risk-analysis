from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig, OmegaConf

__all__ = [
    "load_config",
    "load_dotenv",
    "repo_root",
    "resolve_path",
]

_CONFIG: DictConfig | None = None


def repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "environment.yaml").exists():
            return candidate
    raise FileNotFoundError("environment.yaml not found in any parent directory")


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
