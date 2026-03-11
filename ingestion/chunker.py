import uuid
from typing import List, Optional, Tuple

from pydantic import BaseModel
from extractors.schema import Metadata


class Chunk(BaseModel):
    chunk_id: str
    text: str
    metadata: Metadata
    start_token: Optional[int] = None
    end_token: Optional[int] = None


# naive tokenizer: split on whitespace. In a real system we'd use a proper
# tokenizer matching the LLM's tokenization (e.g., tiktoken) so that the
# chunk sizes correspond to model tokens rather than words.

def _simple_tokenize(text: str) -> List[str]:
    return text.split()


def chunk_text(
    text: str, max_tokens: int = 150, overlap: int = 30
) -> List[Tuple[str, int, int]]:
    """Return list of (chunk_text, start_index, end_index) in token units.

    For safety we require 0 <= overlap < max_tokens. If the input text contains no
    tokens, an empty list is returned. This function intentionally builds a list
    of tuples; callers that want to stream can convert it easily.
    """
    if overlap < 0 or overlap >= max_tokens:
        raise ValueError("overlap must be non-negative and less than max_tokens")

    tokens = _simple_tokenize(text)
    chunks: List[Tuple[str, int, int]] = []
    i = 0
    n = len(tokens)
    while i < n:
        j = min(i + max_tokens, n)
        chunk_tokens = tokens[i:j]
        # avoid huge string concatenations if text is absurdly long by limiting
        # token count (already ensured by j-i <= max_tokens)
        chunk_str = " ".join(chunk_tokens)
        chunks.append((chunk_str, i, j))
        if j >= n:
            break
        i = j - overlap
    return chunks


def chunk_document(
    text: str,
    metadata: Metadata,
    max_tokens: int = 150,
    overlap: int = 30,
) -> List[Chunk]:
    """Create Chunk objects for a piece of text with metadata."""
    raw_chunks = chunk_text(text, max_tokens=max_tokens, overlap=overlap)
    result: List[Chunk] = []
    for chunk_str, start, end in raw_chunks:
        cid = str(uuid.uuid4())
        meta = metadata.copy(deep=True)
        meta.section_title = meta.section_title or ""
        result.append(
            Chunk(
                chunk_id=cid,
                text=chunk_str,
                metadata=meta,
                start_token=start,
                end_token=end,
            )
        )
    return result
