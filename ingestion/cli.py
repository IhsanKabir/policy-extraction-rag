import argparse
import json
import os
from pathlib import Path
from typing import List

from extractors.schema import Metadata
from ingestion.parser import parse_document
from ingestion.chunker import chunk_document


def ingest_folder(
    folder: Path,
    output: Path,
    max_tokens: int = 150,
    overlap: int = 30,
    manifest_path: Path | None = None,
):
    """Walk folder and ingest all supported documents into a JSONL file."""
    records = []
    for root, dirs, files in os.walk(folder):
        for fname in files:
            path = Path(root) / fname
            try:
                text, metadata = parse_document(
                    str(path),
                    manifest_path=str(manifest_path) if manifest_path else None,
                )
            except ValueError:
                continue
            chunks = chunk_document(text, metadata, max_tokens=max_tokens, overlap=overlap)
            for ch in chunks:
                records.append(
                    {
                        "chunk_id": ch.chunk_id,
                        "text": ch.text,
                        "metadata": ch.metadata.dict(),
                        "start_token": ch.start_token,
                        "end_token": ch.end_token,
                    }
                )
    with open(output, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    print(f"Ingested {len(records)} chunks to {output}")


def main():
    parser = argparse.ArgumentParser(description="Ingestion helper")
    parser.add_argument("--input", required=True, help="Folder containing source docs")
    parser.add_argument("--output", required=True, help="JSONL output path")
    parser.add_argument("--max-tokens", type=int, default=150)
    parser.add_argument("--overlap", type=int, default=30)
    parser.add_argument(
        "--manifest",
        help="Optional CSV manifest for metadata enrichment (defaults to data/pilot_source_manifest.csv if omitted)",
    )
    args = parser.parse_args()
    ingest_folder(
        Path(args.input),
        Path(args.output),
        args.max_tokens,
        args.overlap,
        Path(args.manifest) if args.manifest else None,
    )


if __name__ == "__main__":
    main()
