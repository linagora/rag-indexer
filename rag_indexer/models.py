from pydantic import BaseModel, field_validator, model_validator


class RagConn(BaseModel):
    base_url: str
    api_key: str

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        # cozy's rag.url may end with '/': strip it so built URLs never hold '//'.
        return v.rstrip("/")


class ContentSpec(BaseModel):
    file_url: str | None = None


class FileState(BaseModel):
    """What cozy-stack answers on a sync message's state_url: what openRAG must hold."""

    name: str
    md5sum: str
    workspace_ids: list[str] = []
    metadata: dict = {}


class IndexMessage(BaseModel):
    # "upsert" | "delete", or "sync": the stack only names the file, and the
    # consumer reads on state_url which of the two the file needs when it runs.
    action: str
    partition: str
    file_id: str
    doctype: str | None = None
    doc_rev: str | None = None  # CouchDB revision; cozy orders the status callbacks on it
    md5sum: str | None = None
    name: str | None = None
    datetime: str | None = None
    content_type: str | None = None
    app_metadata: dict | None = None
    callback_url: str | None = None
    callback_token: str | None = None  # secret: never logged, never in a URL
    rag: RagConn
    content: ContentSpec | None = None
    state_url: str | None = None
    file_token: str | None = None  # secret: bearer of state_url and of its content
    # Workspaces of the file on openRAG; None leaves its memberships alone.
    workspace_ids: list[str] | None = None

    @model_validator(mode="after")
    def _check_required_fields(self):
        if self.action == "upsert":
            # openRAG picks its parser from the name's extension, and the md5sum is
            # what tells whether the indexed content is up to date.
            missing = [field for field in ("name", "md5sum") if not getattr(self, field)]
            if not (self.content and self.content.file_url):
                missing.append("file_url")
            if missing:
                raise ValueError(f"upsert without {', '.join(missing)}")
        if self.action == "sync" and not (self.state_url and self.file_token):
            raise ValueError("sync without state_url or file_token")
        if self.callback_url and not self.doc_rev:
            # cozy-stack orders the status callbacks on it and refuses one without it.
            raise ValueError("callback_url without doc_rev")
        return self
