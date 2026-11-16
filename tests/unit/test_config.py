import pytest

from rag_indexer.config import parse_rag_api_keys


def test_rag_api_keys_unset(monkeypatch):
    monkeypatch.delenv("RAG_API_KEYS", raising=False)
    assert parse_rag_api_keys() == {}


def test_rag_api_keys_drop_the_trailing_slash(monkeypatch):
    monkeypatch.setenv("RAG_API_KEYS", '{"https://a.example/": "ka", "http://b:8000": "kb"}')
    assert parse_rag_api_keys() == {"https://a.example": "ka", "http://b:8000": "kb"}


@pytest.mark.parametrize("raw", ['["https://a.example"]', '{"https://a.example": ""}', '{"": "k"}',
                                 '{"https://a.example": 1}', "not json", "{}"])
def test_rag_api_keys_invalid(monkeypatch, raw):
    monkeypatch.setenv("RAG_API_KEYS", raw)
    with pytest.raises(ValueError):
        parse_rag_api_keys()
