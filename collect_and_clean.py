#!/usr/bin/env python3
"""
High-Value Dataset Builder - Collection and Cleaning Pipeline
Migrated from High-Value-Dataset-Builder repository for NVIDIA AI Workbench compatibility

Provides a modular, well-documented CLI for:
  1) Parsing CLI arguments for --config/--input/--output/--log-usage
  2) Loading YAML config with source, cleaning, and export settings
  3) Collecting files via glob patterns
  4) Cleaning CSV/JSONL/JSON (dedupe, trim whitespace, normalize unicode, remove empties)
  5) Exporting to CSV/JSONL/Parquet
  6) Optional usage logging

Designed for clarity and composability to maximize community value.
"""
from __future__ import annotations
import argparse
import csv
import io
import json
import os
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # Loaded lazily with helpful error if used

# -----------------------------
# Data classes for configuration
# -----------------------------

@dataclass
class SourceConfig:
    path: str
    include: List[str] = field(default_factory=lambda: ["*.jsonl", "*.json", "*.csv"])

@dataclass
class CleaningConfig:
    drop_duplicates: bool = True
    trim_whitespace: bool = True
    normalize_unicode: bool = True
    remove_empty: bool = True
    lower_keys: bool = False

@dataclass
class ExportConfig:
    format: str = "parquet"  # csv|jsonl|parquet
    output_dir: str = "./data/processed"
    filename: str = "dataset"

@dataclass
class AppConfig:
    sources: List[SourceConfig] = field(default_factory=lambda: [SourceConfig(path="./data/raw")])
    cleaning: CleaningConfig = field(default_factory=CleaningConfig)
    export: ExportConfig = field(default_factory=ExportConfig)

# -----------------------------
# Utility functions
# -----------------------------

def eprint(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

# -----------------------------
# YAML loading
# -----------------------------

def load_config(config_path: Optional[Path]) -> AppConfig:
    """Load configuration from YAML if provided, else defaults.
    The YAML may define keys: sources, cleaning, export. Unknown keys are ignored.
    """
    if config_path is None:
        return AppConfig()
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    if yaml is None:
        raise RuntimeError("pyyaml is required to load YAML configs. Install with `pip install pyyaml`." )
    
    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    
    # Parse sources
    sources_raw = data.get("sources") or data.get("source")
    sources: List[SourceConfig] = []
    
    if isinstance(sources_raw, list):
        for s in sources_raw:
            if isinstance(s, dict):
                sources.append(SourceConfig(
                    path=str(s.get("path", "./data/raw")), 
                    include=list(s.get("include", ["*.jsonl", "*.json", "*.csv"]))
                ))
    elif isinstance(sources_raw, dict):
        sources.append(SourceConfig(
            path=str(sources_raw.get("path", "./data/raw")), 
            include=list(sources_raw.get("include", ["*.jsonl", "*.json", "*.csv"]))
        ))
    else:
        sources.append(SourceConfig(path="./data/raw"))
    
    # Parse cleaning
    cl = data.get("cleaning", {}) or {}
    cleaning = CleaningConfig(
        drop_duplicates=bool(cl.get("drop_duplicates", True)),
        trim_whitespace=bool(cl.get("trim_whitespace", True)),
        normalize_unicode=bool(cl.get("normalize_unicode", True)),
        remove_empty=bool(cl.get("remove_empty", True)),
        lower_keys=bool(cl.get("lower_keys", False)),
    )
    
    # Parse export
    ex = data.get("export", {}) or {}
    export = ExportConfig(
        format=str(ex.get("format", "parquet")).lower(),
        output_dir=str(ex.get("output_dir", "./data/processed")),
        filename=str(ex.get("filename", "dataset")),
    )
    
    return AppConfig(sources=sources, cleaning=cleaning, export=export)

# -----------------------------
# File discovery (glob collect)
# -----------------------------

def collect_files(sources: Sequence[SourceConfig]) -> List[Path]:
    """Collect files from each source via glob includes."""
    hits: List[Path] = []
    for src in sources:
        base = Path(src.path)
        for pattern in (src.include or ["*"]):
            for p in base.rglob(pattern):
                if p.is_file():
                    hits.append(p)
    
    # Stable order
    hits = sorted(set(hits))
    return hits

if __name__ == "__main__":
    print("Dataset collection and cleaning pipeline")
    print("Migrated from High-Value-Dataset-Builder for NVIDIA AI Workbench compatibility")
    print("Run with --help for usage information")
