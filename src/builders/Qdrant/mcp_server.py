"""
MCP toolkit over the protein-function store.

The tools are shaped around how an agent actually works a biomedical question,
which is a funnel rather than a single lookup:

    search_proteins   shortlist proteins, each already carrying its best
                      snippet as evidence for why it surfaced
    read_snippets     pull more windows from one protein, re-scored against a
                      *different* question than the one that surfaced it
    expand_snippet    widen a promising window outward in sentences, when the
                      snippet is suggestive but cut off
    read_record       give up on windows and read the whole function text

Everything after the first call is the agent deciding the initial retrieval did
not settle the question -- which is the behaviour the chunk collection exists to
support, and the reason search runs through MCP here rather than through a
Python call the application would not be able to make.

Identity: every tool speaks UniProt accessions ("P04637"). Point ids are
uuid5 of the accession, because Qdrant takes only integers and UUIDs, but that
hash never surfaces here -- an agent that receives a result can feed its
uniprot_id straight back into the next tool. `entrez_id` rides along on record
results when the store was built with a mapping, as a list of ids; note it is
not unique, since several isoforms of one gene are several records, and that
about 5% of accessions have no entrez id and omit the field.

Run it
------
    BIOBERT_MODEL_PATH=biobert-go-retrieval QDRANT_PATH=qdrant_store \
        python mcp_server.py

Or as a client config entry:

    {"mcpServers": {"protein-retrieval": {
        "command": "python",
        "args": ["C:/Users/Yahya/Documents/BioBERT_Finetune/Qdrant/mcp_server.py"],
        "env": {"QDRANT_PATH": "...", "BIOBERT_MODEL_PATH": "..."}}}}

Embedded mode holds an exclusive lock on the storage folder, so one process at
a time. Point QDRANT_URL at a server instead if the application needs more.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Literal

from core.config import (CHUNKS, RECORDS, Settings, load_encoder,
                        load_sparse_encoder, open_client)
from core.retriever import MODES, Retriever

# The SDK renamed FastMCP to MCPServer in 2.0. The decorator API, the
# `instructions` argument and run(transport=...) are the same in both, so one
# import shim covers the whole file -- worth it, since Colab installs whatever
# is current while an application host may well be pinned to 1.x.
try:
    from mcp.server.mcpserver import MCPServer as _Server   # mcp >= 2
    MCP_MAJOR = 2
except ImportError:                                         # pragma: no cover
    from mcp.server.fastmcp import FastMCP as _Server       # mcp 1.x
    MCP_MAJOR = 1

INSTRUCTIONS = """\
Retrieval over UniProt function text, indexed with a BioBERT encoder fine-tuned
on Gene Ontology term -> function text pairs. Queries phrased as functional
descriptions ("catalyzes transfer of a phosphate group to a histidine residue")
work far better than bare identifiers; use read_record if you already know the
accession and just want its text.

Records are proteins, keyed by UniProt accession. Several isoforms of one gene
are separate records with genuinely different function text, so two hits sharing
an entrez_id are not duplicates.

Start with search_proteins. If a snippet looks relevant but incomplete, call
expand_snippet on it before concluding anything -- snippets are short sentence
windows and routinely cut mid-argument. Use read_snippets when you want to ask a
protein a follow-up question the original query did not cover.
"""


def make_retriever(settings: Settings | None = None) -> Retriever:
    settings = settings or Settings.from_env()
    return Retriever(
        client=open_client(storage=settings.storage, url=settings.url, api_key=settings.api_key),
        encoder=load_encoder(settings.model_path),
        sparse_encoder=load_sparse_encoder(),
        prefetch_limit=settings.prefetch_limit,
        shortlist=settings.shortlist,
        chunk_prefetch_limit=settings.chunk_prefetch_limit,
    )


def build_server(retriever: Retriever | None = None,
                 settings: Settings | None = None):
    """
    A fresh server bound to `retriever`. The evaluation passes one in so that
    BioBERT is loaded once for the whole sweep rather than once per variant;
    __main__ lets it build its own from the environment.
    """
    mcp = _Server("protein-retrieval", instructions=INSTRUCTIONS)
    state: dict[str, Retriever | None] = {"retriever": retriever}

    def r() -> Retriever:
        if state["retriever"] is None:
            state["retriever"] = make_retriever(settings)
        return state["retriever"]

    # Every tool returns a single object rather than a bare list. Two reasons:
    # an agent reads "8 proteins, mode=chunk_rerank" and knows what it is looking
    # at without inferring it, and the SDK serializes a list return as one
    # content block per element, which callers then have to reassemble.

    @mcp.tool()
    def search_proteins(query: str, k: int = 10, snippets_per_record: int = 1,
                        mode: Literal["chunk_rerank", "record_hybrid", "dense", "sparse",
                                      "chunk_max", "chunk_top2", "chunk_lognorm",
                                      "chunk_rrf"] = "record_hybrid") -> dict[str, Any]:
        """Find proteins whose function text matches a functional description.

        Args:
            query: A functional description, not an identifier. Free text.
            k: How many proteins to return (default 10).
            snippets_per_record: Best-matching sentence windows to attach as
                evidence per protein (default 1). Raise it when you need to
                compare several passages within one protein.
            mode: record_hybrid (default) ranks by whole-record match, which is
                the measured-best ordering. The chunk_* modes re-rank by an
                aggregate of passage scores instead; they are experimental and
                have not been shown to beat the default. dense / sparse are
                single-signal diagnostics.
                Note the snippets come back either way -- the mode changes the
                ORDER of the proteins, not whether you get passage evidence.

        Returns `results`: one entry per protein with its uniprot_id, scores,
        total snippet count, and the snippets themselves. A snippet's chunk_idx
        is what expand_snippet takes; the uniprot_id is what every other tool
        takes.
        """
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        results = r().search(query, k=k, snippets_per_record=snippets_per_record, mode=mode)
        return {"query": query, "mode": mode, "n_results": len(results), "results": results}

    @mcp.tool()
    def read_snippets(uniprot_id: str, query: str, n: int = 3) -> dict[str, Any]:
        """Re-query one protein's passages against a new question.

        Use this when a protein from search_proteins looks promising but you
        want to ask it something the original query did not cover -- the
        passages are re-scored against `query`, so a different question surfaces
        different windows of the same text.

        Args:
            uniprot_id: UniProt accession, exactly as returned by search_proteins.
            query: The follow-up question, as free text.
            n: How many passages to return (default 3).
        """
        snippets = r().snippets(uniprot_id, query, n=n)
        return {"uniprot_id": uniprot_id, "query": query,
                "n_results": len(snippets), "snippets": snippets}

    @mcp.tool()
    def expand_snippet(uniprot_id: str, chunk_idx: int, radius: int = 1) -> dict[str, Any]:
        """Widen a snippet outward by `radius` sentences on each side.

        Snippets are fixed-size sentence windows and frequently truncate a claim
        mid-argument. Expand before drawing a conclusion from a snippet that
        reads as incomplete; expand again with a larger radius if it still does.

        Args:
            uniprot_id: Accession the snippet belongs to.
            chunk_idx: The snippet's chunk_idx, from search_proteins or read_snippets.
            radius: Extra sentences on each side (default 1).

        Returns the widened text plus its [start, end) sentence span, the
        record's total sentence count, and at_start/at_end flags telling you
        whether expanding further would add anything.
        """
        return r().expand(uniprot_id, chunk_idx, radius=radius)

    @mcp.tool()
    def read_record(uniprot_id: str) -> dict[str, Any]:
        """Read one protein's complete function text.

        The fallback when windowed passages are not enough, and the direct route
        when you already know which accession you care about.

        Args:
            uniprot_id: UniProt accession.
        """
        return r().read_record(uniprot_id)

    @mcp.tool()
    def collection_info() -> dict[str, Any]:
        """Report what is indexed: point counts and available search modes.

        Worth calling once if results look empty or surprising -- it distinguishes
        an unpopulated store from a query that genuinely matched nothing.
        """
        client = r().client
        out: dict[str, Any] = {"modes": list(MODES), "record_key": "uniprot_id"}
        for name in (RECORDS, CHUNKS):
            if not client.collection_exists(name):
                out[name] = {"exists": False}
                continue
            out[name] = {"exists": True, "points": client.get_collection(name).points_count}
        return out

    return mcp


def is_error(result) -> bool:
    """1.x spells it isError, 2.x spells it is_error."""
    return bool(getattr(result, "is_error", False) or getattr(result, "isError", False))


def unwrap(result) -> dict:
    """
    An MCP tool result -> the dict the tool returned.

    Structured content when the SDK provides it (1.x camelCase, 2.x snake_case),
    otherwise the JSON text block. Every tool in mcp_server.py returns a single
    object precisely so this stays unambiguous: a list return is serialized as
    one content block per element, and a one-element list is then
    indistinguishable from a single object.
    """
    for attr in ("structured_content", "structuredContent"):
        structured = getattr(result, attr, None)
        if isinstance(structured, dict):
            return structured.get("result", structured)

    blocks = [b for b in (getattr(result, "content", None) or []) if getattr(b, "text", None)]
    if not blocks:
        return {}
    if len(blocks) > 1:
        raise ValueError(f"Expected one content block from the tool, got {len(blocks)} -- "
                         f"a tool is returning a bare list instead of an object.")
    return json.loads(blocks[0].text)


@asynccontextmanager
async def open_session(server):
    """
    A connected client for `server` over the SDK's in-memory transport.

    This is what lets the evaluation claim it measures the MCP path rather than
    the Python one: real tool schemas, real argument validation, real JSON
    round-trip -- just no subprocess and no pipe. Yields an object exposing
    list_tools() and call_tool(), which both SDK generations provide.
    """
    if MCP_MAJOR >= 2:
        from mcp.client import Client
        async with Client(server) as client:
            yield client
    else:                                                   # pragma: no cover
        from mcp.shared.memory import create_connected_server_and_client_session
        async with create_connected_server_and_client_session(server._mcp_server) as session:
            yield session


if __name__ == "__main__":
    build_server().run(transport="stdio")
