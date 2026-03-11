import os

import fitz

from ingestion.parser import parse_document
from extractors.schema import SourceType


def create_sample_pdf(path: str, text: str):
    # create a minimal PDF with the given text so tests can run offline
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    doc.save(path)
    doc.close()


def create_sample_html(path: str, text: str):
    html = f"""<html><body><p>{text}</p></body></html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def test_parse_pdf(tmp_path):
    file_path = tmp_path / "test.pdf"
    create_sample_pdf(str(file_path), "Hello PDF world")

    text, metadata = parse_document(str(file_path))
    assert "Hello PDF" in text
    assert metadata.document_id == "test.pdf"
    assert metadata.source_id.endswith("test.pdf")
    # default document_type in parser is fare_rule
    assert metadata.document_type == SourceType.fare_rule


def test_parse_html(tmp_path):
    file_path = tmp_path / "test.html"
    create_sample_html(str(file_path), "Some HTML content")

    text, metadata = parse_document(str(file_path))
    assert "Some HTML content" in text
    assert metadata.document_id == "test.html"
    assert metadata.source_id.endswith("test.html")
    assert metadata.document_type == SourceType.terms_page


def test_parse_html_enriched_from_manifest(tmp_path):
    file_path = tmp_path / "supplier_offer_ExampleSupplier_2026-02-18_OFFER-042.html"
    create_sample_html(str(file_path), "Offer content")

    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "\n".join(
            [
                "source_id,document_id,document_type,carrier,supplier,offer_or_circular_id,source_url,source_filename,file_format,market,route_scope,cabin,fare_family,effective_from,effective_to,language,status,notes",
                "src_offer_42,doc_offer_42,supplier_offer,,ExampleSupplier,OFFER-042,,supplier_offer_ExampleSupplier_2026-02-18_OFFER-042.html,html,US,International,mixed,,2026-02-18,2026-03-31,en,pending_parse,",
            ]
        ),
        encoding="utf-8",
    )

    text, metadata = parse_document(str(file_path), manifest_path=str(manifest))
    assert "Offer content" in text
    assert metadata.document_type == SourceType.supplier_offer
    assert metadata.source_id == "src_offer_42"
    assert metadata.document_id == "doc_offer_42"
    assert metadata.supplier == "ExampleSupplier"
    assert metadata.offer_or_circular_id == "OFFER-042"
    assert metadata.market == "US"
    assert metadata.route_scope == "International"
    assert metadata.cabin == "mixed"
    assert metadata.effective_from == "2026-02-18"
    assert metadata.effective_to == "2026-03-31"


def test_unsupported_file(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("just text")
    try:
        parse_document(str(file_path))
        assert False, "Expected ValueError for unsupported file type"
    except ValueError:
        pass
