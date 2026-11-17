import pytest

from rag_indexer.config import parse_rag_server


def test_rag_server_unset(monkeypatch):
    monkeypatch.delenv("RAG_BASE_URL", raising=False)
    monkeypatch.delenv("RAG_API_KEY", raising=False)
    assert parse_rag_server() == ("", "")


def test_rag_server_drops_the_trailing_slash(monkeypatch):
    monkeypatch.setenv("RAG_BASE_URL", "https://a.example/")
    monkeypatch.setenv("RAG_API_KEY", "k")
    assert parse_rag_server() == ("https://a.example", "k")


@pytest.mark.parametrize("url, key", [("https://a.example", ""), ("", "k"), ("/", "k")])
def test_rag_server_needs_both(monkeypatch, url, key):
    monkeypatch.setenv("RAG_BASE_URL", url)
    monkeypatch.setenv("RAG_API_KEY", key)
    with pytest.raises(ValueError):
        parse_rag_server()
