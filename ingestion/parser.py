import csv
import functools
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import fitz  # PyMuPDF
from bs4 import BeautifulSoup

from extractors.schema import Metadata, SourceType


def _empty_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_manifest_path() -> Path:
    return _repo_root() / "data" / "pilot_source_manifest.csv"


@functools.lru_cache(maxsize=4)
def _load_manifest_rows(manifest_path: str) -> Dict[str, dict]:
    """Index manifest rows by source filename for metadata enrichment."""
    path = Path(manifest_path)
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = {}
        for row in reader:
            source_filename = _empty_to_none(row.get("source_filename"))
            if source_filename:
                rows[source_filename.lower()] = row
        return rows


def _safe_source_type(raw_value: Optional[str], fallback: SourceType) -> SourceType:
    value = _empty_to_none(raw_value)
    if not value:
        return fallback
    try:
        return SourceType(value)
    except ValueError:
        return fallback


def _enrich_from_manifest(file_path: str, metadata: Metadata, manifest_path: Optional[str] = None) -> Metadata:
    """Override placeholder metadata with pilot manifest values when available."""
    manifest = str(Path(manifest_path) if manifest_path else _default_manifest_path())
    rows = _load_manifest_rows(manifest)
    if not rows:
        return metadata

    basename = os.path.basename(file_path).lower()
    row = rows.get(basename)
    if row is None:
        return metadata

    enriched = metadata.copy(deep=True)
    enriched.source_id = _empty_to_none(row.get("source_id")) or enriched.source_id
    enriched.document_id = _empty_to_none(row.get("document_id")) or enriched.document_id
    enriched.document_type = _safe_source_type(row.get("document_type"), enriched.document_type)
    enriched.carrier = _empty_to_none(row.get("carrier")) or enriched.carrier
    enriched.supplier = _empty_to_none(row.get("supplier")) or enriched.supplier
    enriched.offer_or_circular_id = _empty_to_none(row.get("offer_or_circular_id")) or enriched.offer_or_circular_id
    enriched.market = _empty_to_none(row.get("market")) or enriched.market
    enriched.route_scope = _empty_to_none(row.get("route_scope")) or enriched.route_scope
    enriched.cabin = _empty_to_none(row.get("cabin")) or enriched.cabin
    enriched.fare_family = _empty_to_none(row.get("fare_family")) or enriched.fare_family
    enriched.effective_from = _empty_to_none(row.get("effective_from")) or enriched.effective_from
    enriched.effective_to = _empty_to_none(row.get("effective_to")) or enriched.effective_to
    enriched.source_url = _empty_to_none(row.get("source_url")) or enriched.source_url
    enriched.source_filename = _empty_to_none(row.get("source_filename")) or enriched.source_filename
    return enriched


def parse_pdf(file_path: str) -> Tuple[str, Metadata]:
    """Open a PDF and extract all text, returning text + minimal metadata.

    This is a very basic implementation; later we'll enrich metadata from file
    names, directory structure, or a companion manifest.
    """
    doc = fitz.open(file_path)
    text_chunks = []
    for page in doc:
        text_chunks.append(page.get_text())

    text = "\n".join(text_chunks)
    metadata = Metadata(
        source_id=os.path.abspath(file_path),
        document_id=os.path.basename(file_path),
        document_type=SourceType.fare_rule,  # placeholder default
        source_filename=os.path.basename(file_path),
    )
    return text, _enrich_from_manifest(file_path, metadata)


def parse_html(file_path: str) -> Tuple[str, Metadata]:
    """Load an HTML/HTM file and return the visible text."""
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # get visible text separated by newlines for better chunking later
    text = soup.get_text(separator="\n")
    metadata = Metadata(
        source_id=os.path.abspath(file_path),
        document_id=os.path.basename(file_path),
        document_type=SourceType.terms_page,  # placeholder default
        source_filename=os.path.basename(file_path),
    )
    return text, _enrich_from_manifest(file_path, metadata)


def parse_document(file_path: str, manifest_path: Optional[str] = None) -> Tuple[str, Metadata]:
    ext = file_path.lower().split(".")[-1]
    if ext == "pdf":
        text, metadata = parse_pdf(file_path)
    elif ext in ("html", "htm"):
        text, metadata = parse_html(file_path)
    else:
        raise ValueError(f"Unsupported file type for ingestion: {file_path}")

    # Optional explicit manifest path is useful in tests and non-default layouts.
    if manifest_path:
        metadata = _enrich_from_manifest(file_path, metadata, manifest_path=manifest_path)
    return text, metadata
