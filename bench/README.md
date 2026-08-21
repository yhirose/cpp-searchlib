# Benchmarks

`searchlib-bench` (built by CMake, run it with `just bench`) measures one
corpus against one query set and reports what an index costs to build, to
ship, and to query. Everything is an option, so the same binary covers the
small English corpus the optimization work was tuned against and a corpus
large enough that building it is a memory problem.

```
just bench                      # KJV x10, the regression baseline
build/bench/searchlib-bench --help
```

The other files here exist for one job: comparing against another engine on a
Japanese corpus, honestly.

## Why the corpus is pre-tokenized

cpp-searchlib segments Japanese with cpp-segmentlib; Tantivy would use
Lindera. Different segmenters produce different terms, so a direct comparison
measures the two segmenters as much as the two engines, and the hit counts
never line up.

So the corpus is segmented **once**, by cpp-searchlib, into space-separated
terms, and both engines index that with a whitespace tokenizer. Reading the
result back through `UTF8PlainTextTokenizer` reproduces the term count
exactly, which is the check that the round trip loses nothing. Tokenization
cost is then measured separately rather than folded into either engine's
build time.

The queries are written in the same pre-segmented form, in one file both
engines read. **The hit counts are the correctness gate**: if they differ,
the two engines are not answering the same question and no latency number
below them means anything. `bench/tantivy` aligns Tantivy's query parser with
cpp-searchlib's grammar (space is And, `|` is Or) for exactly this reason.

## Building the corpus

```sh
python3 bench/aozora.py fetch   /tmp/aozora                 # ~700 MB of zips
python3 bench/aozora.py extract /tmp/aozora /tmp/aozora.tsv

clang++ -O2 -DNDEBUG -std=c++17 -I include -I src -isystem third_party \
  bench/segment_corpus.cpp src/*.cpp -o /tmp/segment_corpus
/tmp/segment_corpus --in /tmp/aozora.tsv --out /tmp/aozora_seg.tsv \
  --splitter test/models/ja-ud-gsd.mod --chunk 300
```

`--chunk 300` matters: Aozora Bunko is a few thousand book-length works, and
picking the top 10 out of a few thousand documents does not exercise ranking.
Cutting each work into 300-term passages gives a document count that does.

## Running both engines

```sh
build/bench/searchlib-bench --corpus /tmp/aozora_seg.tsv --text-field 3 \
  --repeat 1 --queries bench/queries_ja.txt --text-ranges skip

# add --index /tmp/aozora.idx to measure the compressed on-disk index
# instead of the in-memory one; they are very different animals

cd bench/tantivy && cargo build --release
./target/release/tantivy-bench --corpus /tmp/aozora_seg.tsv --text-field 3 \
  --queries ../queries_ja.txt --index-dir /tmp/tantivy-index
```

## What the three configurations mean

|                | cpp in-memory | cpp compressed | Tantivy |
|----------------|---------------|----------------|---------|
| postings       | plain arrays  | Elias-Fano     | block codec |
| where it lives | RAM, expanded | RAM, compressed| mmap, page cache |
| built by       | indexing, or `load()` | `load_compressed_index()` | its own writer |

These are not three points on one curve. The in-memory index is the fast one
and the expensive one; the compressed backend trades a large constant factor
on every posting access for a much smaller footprint. Reporting only one of
them would misrepresent where the library stands.
