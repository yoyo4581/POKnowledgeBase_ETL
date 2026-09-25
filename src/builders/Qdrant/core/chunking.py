"""
Sentence splitting and sentence-window chunking for UniProt function text.

Deterministic by design. expand_snippet() resolves a chunk by
uuid5(uniprot_id + "#" + idx) and then slices the record's stored `sentences`
with the chunk's [sent_start, sent_end) -- so if the splitter disagreed
between the machine that built the collection and the machine that queries
it, expansion would silently return the wrong text rather than error. A
pure-regex splitter with no model files and no version-sensitive dependency
is what keeps Colab and the ETL host in lockstep; the splitter name is
recorded in the export config so a future change is detectable.

Why no sentence tokenizer library: UniProt function prose is dense with
constructs that generic splitters get wrong anyway -- "E. coli", "e.g.",
"approx. 3 kDa", "(PubMed:12345678)." -- so the abbreviation set below is
doing the real work regardless of which engine wraps it.
"""
from __future__ import annotations

import re

SPLITTER_NAME = "biomed-regex-v1"

# Tokens that end in a period without ending a sentence. Lowercased on lookup.
_ABBREVIATIONS = frozenset("""
e.g i.e cf ca approx ca vs resp etc et al fig figs tab ref refs eq eqs
no nos vol pp sec min h hr hrs kda da mol mm um nm pm ph
sp spp subsp var str cv gen fam ord
st dr prof mr ms jr sr inc ltd
""".split())

# A period/!/? followed by optional closing bracket-or-quote, then whitespace.
_CANDIDATE = re.compile(r"[.!?]+[)\]\"'’”]*\s+")

# What a real sentence is allowed to start with.
_OPENS_SENTENCE = re.compile(r"[(\[\"'‘“A-Z0-9]")


def split_sentences(text: str) -> list[str]:
    """
    Split `text` into sentences, never returning an empty list for non-empty
    input -- a record with one unsplittable blob still needs one sentence so
    that chunking and expansion have something to index.
    """
    text = (text or "").strip()
    if not text:
        return []

    sentences: list[str] = []
    start = 0
    for match in _CANDIDATE.finditer(text):
        head = text[start:match.start()]
        token = re.split(r"[\s(\[]", head)[-1].rstrip(".")

        if token.lower() in _ABBREVIATIONS:
            continue
        if re.fullmatch(r"[A-Z]", token):        # "E." in E. coli, "C." in C. elegans
            continue
        if re.fullmatch(r"\d+", token):          # "1." opening an enumerated clause
            continue

        nxt = text[match.end():match.end() + 1]
        if nxt and not _OPENS_SENTENCE.match(nxt):
            continue

        sentence = text[start:match.end()].strip()
        if sentence:
            sentences.append(sentence)
        start = match.end()

    tail = text[start:].strip()
    if tail:
        sentences.append(tail)
    return sentences or [text]


def sentence_windows(sentences: list[str], window: int = 3, stride: int = 2) -> list[dict]:
    """
    Overlapping sentence windows, each carrying the [sent_start, sent_end) span
    it came from so expand_snippet() can widen it against the record later.

    window/stride is the experiment's main knob. Smaller stride = more, more
    heavily overlapping chunks = finer localization of the signal, at linear
    cost in embedding time and storage. A window that covers the whole record
    collapses the chunk collection back into the record collection, which is
    the degenerate case where chunk re-ranking can show no benefit.
    """
    if window < 1 or stride < 1:
        raise ValueError(f"window and stride must be >= 1 (got window={window}, stride={stride})")
    if not sentences:
        return []
    if len(sentences) <= window:
        return [{"chunk_idx": 0, "sent_start": 0, "sent_end": len(sentences),
                 "text": " ".join(sentences)}]

    chunks = []
    for start in range(0, len(sentences) - window + 1, stride):
        end = start + window
        chunks.append({"chunk_idx": len(chunks), "sent_start": start, "sent_end": end,
                       "text": " ".join(sentences[start:end])})

    # A trailing stride that doesn't divide evenly would drop the last few
    # sentences entirely; anchor a final window to the end when that happens.
    if chunks[-1]["sent_end"] < len(sentences):
        start = len(sentences) - window
        chunks.append({"chunk_idx": len(chunks), "sent_start": start, "sent_end": len(sentences),
                       "text": " ".join(sentences[start:])})
    return chunks


def chunk_text(text: str, window: int = 3, stride: int = 2) -> tuple[list[str], list[dict]]:
    """Convenience: raw text -> (sentences, chunks). Both get stored on the record."""
    sentences = split_sentences(text)
    return sentences, sentence_windows(sentences, window=window, stride=stride)
