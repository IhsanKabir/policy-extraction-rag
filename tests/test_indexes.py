import pytest

from ingestion.indexes import VectorIndex, KeywordIndex


def test_vector_index_creation(monkeypatch):
    # create dummy qdrant_client module so imports inside VectorIndex.__init__ succeed
    import types, sys

    dummy = types.SimpleNamespace()
    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def recreate_collection(self, *args, **kwargs):
            pass
    # also need Distance.COSINE attribute
    dummy.QdrantClient = DummyClient
    dummy.http = types.SimpleNamespace(models=types.SimpleNamespace(Distance=types.SimpleNamespace(COSINE="cosine")))
    sys.modules["qdrant_client"] = dummy
    sys.modules["qdrant_client.http"] = dummy.http
    sys.modules["qdrant_client.http.models"] = dummy.http.models

    idx = VectorIndex("test_collection", host="fake", port=1234)
    assert idx.collection_name == "test_collection"


def test_vector_index_upsert_defaults_payloads(monkeypatch):
    import sys
    import types

    captured = {}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def create_collection(self, *args, **kwargs):
            captured["create_collection"] = (args, kwargs)

        def collection_exists(self, *args, **kwargs):
            return False

        def upsert(self, *args, **kwargs):
            captured["upsert"] = (args, kwargs)

    dummy = types.SimpleNamespace()
    dummy.QdrantClient = DummyClient
    dummy.http = types.SimpleNamespace(
        models=types.SimpleNamespace(
            Distance=types.SimpleNamespace(COSINE="cosine"),
            VectorParams=lambda size, distance: {"size": size, "distance": distance},
        )
    )
    sys.modules["qdrant_client"] = dummy
    sys.modules["qdrant_client.http"] = dummy.http
    sys.modules["qdrant_client.http.models"] = dummy.http.models

    idx = VectorIndex("test_collection", host="fake", port=1234)
    idx.upsert(ids=["a", "b"], vectors=[[0.1], [0.2]])

    _, kwargs = captured["upsert"]
    points = kwargs["points"]
    assert len(points) == 2
    assert points[0]["payload"] == {}
    assert points[1]["payload"] == {}


def test_vector_index_does_not_recreate_by_default(monkeypatch):
    import sys
    import types

    calls = {"recreate": 0, "create": 0}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def recreate_collection(self, *args, **kwargs):
            calls["recreate"] += 1

        def collection_exists(self, *args, **kwargs):
            return True

        def create_collection(self, *args, **kwargs):
            calls["create"] += 1

    dummy = types.SimpleNamespace()
    dummy.QdrantClient = DummyClient
    dummy.http = types.SimpleNamespace(
        models=types.SimpleNamespace(
            Distance=types.SimpleNamespace(COSINE="cosine"),
            VectorParams=lambda size, distance: {"size": size, "distance": distance},
        )
    )
    sys.modules["qdrant_client"] = dummy
    sys.modules["qdrant_client.http"] = dummy.http
    sys.modules["qdrant_client.http.models"] = dummy.http.models

    VectorIndex("test_collection", host="fake", port=1234)
    assert calls["recreate"] == 0
    assert calls["create"] == 0


def test_keyword_index_creation(monkeypatch):
    import sys
    import types

    class DummyOS:
        def __init__(self, *args, **kwargs):
            pass

        class indices:
            @staticmethod
            def exists(index):
                return False

            @staticmethod
            def create(index):
                pass

        def search(self, *args, **kwargs):
            return {"hits": {"hits": []}}

        def index(self, *args, **kwargs):
            pass

    dummy_module = types.SimpleNamespace(OpenSearch=DummyOS)
    monkeypatch.setitem(sys.modules, "opensearchpy", dummy_module)
    idx = KeywordIndex(host="fake", port=1234, index_name="idx")
    assert idx.index_name == "idx"
