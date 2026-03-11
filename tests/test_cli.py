import json
from pathlib import Path

import pytest

from ingestion.cli import ingest_folder
from ingestion.chunker import Chunk


def test_ingest_folder(tmp_path, monkeypatch):
    # create fake docs
    pdf_path = tmp_path / "doc.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")
    html_path = tmp_path / "doc.html"
    html_path.write_text("<html><body>text</body></html>")

    # monkeypatch parser to return known text & metadata
    from ingestion import parser

    def fake_parse(path, manifest_path=None):
        return "a b c d e f g h i j", parser.Metadata(
            source_id=str(path),
            document_id=Path(path).name,
            document_type=parser.SourceType.terms_page,
        )

    monkeypatch.setattr("ingestion.cli.parse_document", fake_parse)
    monkeypatch.setattr("ingestion.cli.chunk_document", lambda t, m, max_tokens, overlap: [
        Chunk(chunk_id="1", text="a b c", metadata=m, start_token=0, end_token=3)
    ])

    output = tmp_path / "out.jsonl"
    ingest_folder(tmp_path, output)
    lines = output.read_text().strip().splitlines()
    assert len(lines) == 2
    rec = json.loads(lines[0])
    assert rec["chunk_id"] == "1"


def test_ingest_folder_passes_manifest(tmp_path, monkeypatch):
    html_path = tmp_path / "doc.html"
    html_path.write_text("<html><body>text</body></html>")

    from ingestion import parser

    calls = []

    def fake_parse(path, manifest_path=None):
        calls.append((Path(path).name, manifest_path))
        return "a b c", parser.Metadata(
            source_id=str(path),
            document_id=Path(path).name,
            document_type=parser.SourceType.terms_page,
        )

    monkeypatch.setattr("ingestion.cli.parse_document", fake_parse)
    monkeypatch.setattr(
        "ingestion.cli.chunk_document",
        lambda t, m, max_tokens, overlap: [Chunk(chunk_id="1", text=t, metadata=m, start_token=0, end_token=3)],
    )

    output = tmp_path / "out.jsonl"
    manifest = tmp_path / "manifest.csv"
    manifest.write_text("source_id,document_id,document_type,source_filename\n", encoding="utf-8")

    ingest_folder(tmp_path, output, manifest_path=manifest)
    assert calls
    assert calls[0][1] == str(manifest)
