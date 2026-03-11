from ingestion.chunker import chunk_text, chunk_document
from extractors.schema import Metadata, SourceType


SAMPLE_TEXT = "This is a sentence. " * 20  # smaller sample to avoid memory issues


def make_metadata():
    return Metadata(
        source_id="/path/doc.pdf",
        document_id="doc.pdf",
        document_type=SourceType.fare_rule,
    )


def test_chunk_text_length():
    chunks = chunk_text(SAMPLE_TEXT, max_tokens=50, overlap=10)
    # ensure no chunk exceeds max_tokens when tokenized
    for chunk, start, end in chunks:
        tokens = chunk.split()
        assert len(tokens) <= 50
    # overlap check: consecutive chunks should overlap by 10 tokens
    for i in range(1, len(chunks)):
        prev_end = chunks[i - 1][2]
        curr_start = chunks[i][1]
        assert curr_start == prev_end - 10


def test_chunk_document_metadata():
    text = "one two three " * 20
    meta = make_metadata()
    chunks = chunk_document(text, meta, max_tokens=15, overlap=5)
    assert chunks
    for ch in chunks:
        assert ch.metadata.document_id == "doc.pdf"
        assert ch.chunk_id
        assert ch.start_token is not None and ch.end_token is not None
