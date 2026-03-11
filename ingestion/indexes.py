from typing import Any, List, Optional

# postpone heavy imports until runtime to avoid compatibility issues during tests


class VectorIndex:
    def __init__(
        self,
        collection_name: str,
        host: str = "localhost",
        port: int = 6333,
        api_key: Optional[str] = None,
        vector_size: int = 768,
        recreate: bool = False,
    ):
        # import inside initializer
        from qdrant_client import QdrantClient
        from qdrant_client.http.models import Distance

        try:
            from qdrant_client.http.models import VectorParams
        except Exception:  # pragma: no cover - compatibility fallback
            VectorParams = None

        self.client = QdrantClient(url=f"http://{host}:{port}", api_key=api_key)
        self.collection_name = collection_name
        self._ensure_collection(
            collection_name=collection_name,
            vector_size=vector_size,
            distance=Distance.COSINE,
            recreate=recreate,
            vector_params_cls=VectorParams,
        )

    def _ensure_collection(self, collection_name: str, vector_size: int, distance: Any, recreate: bool, vector_params_cls: Any):
        if recreate and hasattr(self.client, "recreate_collection"):
            self.client.recreate_collection(
                collection_name,
                vector_size=vector_size,
                distance=distance,
            )
            return

        exists = False
        if hasattr(self.client, "collection_exists"):
            try:
                exists = bool(self.client.collection_exists(collection_name=collection_name))
            except TypeError:
                exists = bool(self.client.collection_exists(collection_name))
        elif hasattr(self.client, "get_collection"):
            try:
                self.client.get_collection(collection_name=collection_name)
                exists = True
            except Exception:
                exists = False

        if exists or not hasattr(self.client, "create_collection"):
            return

        if vector_params_cls is not None:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=vector_params_cls(size=vector_size, distance=distance),
            )
        else:  # pragma: no cover - fallback for older qdrant client signatures
            self.client.create_collection(
                collection_name,
                vector_size=vector_size,
                distance=distance,
            )

    def upsert(self, ids: List[str], vectors: List[List[float]], payloads: Optional[List[Any]] = None):
        if len(ids) != len(vectors):
            raise ValueError("ids and vectors must have the same length")

        if payloads is None:
            payloads = [{} for _ in ids]
        elif len(payloads) != len(ids):
            raise ValueError("payloads must be None or have the same length as ids")

        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                {"id": _id, "vector": vec, "payload": payload}
                for _id, vec, payload in zip(ids, vectors, payloads)
            ],
        )

    def search(self, vector: List[float], top_k: int = 10, filter: Optional[Any] = None):
        return self.client.search(
            collection_name=self.collection_name,
            query_vector=vector,
            limit=top_k,
            filter=filter,
        )


class KeywordIndex:
    """Simple wrapper around OpenSearch pure text indexing."""

    def __init__(self, host: str = "localhost", port: int = 9200, index_name: str = "documents"):
        from opensearchpy import OpenSearch

        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_compress=True,
        )
        self.index_name = index_name
        # create index if not exists
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name)

    def index(self, doc_id: str, text: str, metadata: dict):
        body = {"text": text, "metadata": metadata}
        self.client.index(index=self.index_name, id=doc_id, body=body)

    def search(self, query: str, top_k: int = 10):
        body = {"query": {"match": {"text": query}}}
        resp = self.client.search(index=self.index_name, body=body, size=top_k)
        hits = resp.get("hits", {}).get("hits", [])
        return hits
