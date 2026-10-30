"""Runbook retrieval: markdown split by heading into chunks, ranked with BM25 (implemented here, no deps).

Dense embeddings are the planned second retriever (hybrid + rerank); BM25 alone is a strong baseline for
log/error-string queries, where exact tokens like 'OOMKilled' matter more than semantics.
"""
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

STOP = set("the a an of to in on is are was for and or with that this it be as at by from".split())


def tokenize(text):
    return [t for t in re.findall(r"[a-z0-9_]+", text.lower()) if t not in STOP and len(t) > 1]


@dataclass
class Chunk:
    source: str
    heading: str
    text: str


class RunbookIndex:
    def __init__(self, chunks, k1=1.5, b=0.75):
        self.chunks, self.k1, self.b = chunks, k1, b
        self.tf = [Counter(tokenize(c.heading + " " + c.text)) for c in chunks]
        self.len = [sum(t.values()) for t in self.tf]
        self.avg = (sum(self.len) / len(self.len)) if self.len else 0
        df = Counter(w for t in self.tf for w in t)
        n = len(chunks)
        self.idf = {w: math.log(1 + (n - d + 0.5) / (d + 0.5)) for w, d in df.items()}

    @classmethod
    def from_dir(cls, path):
        chunks = []
        for f in sorted(Path(path).glob("*.md")):
            title, buf, head = f.stem, [], f.stem
            for line in f.read_text(encoding="utf-8").splitlines():
                if line.startswith("#"):
                    if buf and "".join(buf).strip():
                        chunks.append(Chunk(f.name, head, "\n".join(buf).strip()))
                    head, buf = line.lstrip("# ").strip(), []
                else:
                    buf.append(line)
            if buf and "".join(buf).strip():
                chunks.append(Chunk(f.name, head, "\n".join(buf).strip()))
        return cls(chunks)

    def search(self, query, k=3):
        q = Counter(tokenize(query))
        scored = []
        for i, tf in enumerate(self.tf):
            s = 0.0
            for w in q:
                if w in tf:
                    f = tf[w]
                    s += self.idf[w] * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * self.len[i] / self.avg))
            if s > 0:
                scored.append((s, i))
        scored.sort(reverse=True)
        return [{"source": self.chunks[i].source, "heading": self.chunks[i].heading,
                 "text": self.chunks[i].text, "score": round(s, 3)} for s, i in scored[:k]]
