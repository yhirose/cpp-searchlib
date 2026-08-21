#!/usr/bin/env python3
"""Build a benchmark corpus out of Aozora Bunko.

    python3 bench/aozora.py fetch   WORKDIR          # catalog + text zips
    python3 bench/aozora.py extract WORKDIR OUT.tsv  # zips -> one TSV

`fetch` is resumable: it skips anything already on disk, so an interrupted
run continues where it stopped. It asks for six files at a time and
identifies itself, because this is a volunteer-run archive.

`extract` writes `id \t title \t author \t text`, one line per work. Aozora's
plain text is Shift_JIS and carries a light markup that would otherwise
become index terms:

    ruby          ｜漢字《かんじ》   -> 漢字
    annotations   ［＃改ページ］     -> dropped

The header block and the 底本 trailer are dropped too, so that bibliographic
boilerplate does not dominate the term statistics of the shorter works.

The result is a corpus of a few thousand very long documents. For a ranked
search benchmark that is the wrong shape -- picking the top 10 out of a few
thousand is not a choice -- so pass it through bench/segment_corpus.cpp with
--chunk to cut each work into pieces first. That tool also pre-tokenizes,
which is what lets another engine index the same terms.
"""

import csv
import io
import re
import subprocess
import sys
import zipfile
from pathlib import Path

CATALOG_URL = ("https://www.aozora.gr.jp/index_pages/"
               "list_person_all_extended_utf8.zip")
USER_AGENT = "cpp-searchlib-benchmark/1.0 (corpus build; contact via repo)"

RUBY = re.compile(r"《[^》]*》")
RUBY_MARK = re.compile(r"｜")
ANNOTATION = re.compile(r"［＃[^］]*］")
SEPARATOR = re.compile(r"^-{10,}$")


def fetch(workdir: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    zips = workdir / "zips"
    zips.mkdir(exist_ok=True)

    catalog = workdir / "catalog.zip"
    if not catalog.exists():
        subprocess.run(["curl", "-sS", "--max-time", "300", "-A", USER_AGENT,
                        "-o", str(catalog), CATALOG_URL], check=True)
    with zipfile.ZipFile(catalog) as z:
        name = next(n for n in z.namelist() if n.endswith(".csv"))
        csv_bytes = z.read(name)

    works = {}
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")))
    for row in reader:
        work_id = row["作品ID"]
        url = row.get("テキストファイルURL", "") or ""
        if work_id in works or not url.endswith(".zip"):
            continue
        works[work_id] = (row["作品名"], row["姓"] + row["名"], url)

    index = workdir / "works.tsv"
    with index.open("w") as out:
        for work_id, (title, author, url) in sorted(works.items()):
            out.write(f"{work_id}\t{title}\t{author}\t{url}\n")
    print(f"{len(works)} works listed in {index}")

    todo = [(w, u) for w, (_, _, u) in works.items()
            if not (zips / f"{w}.zip").exists()]
    print(f"{len(todo)} to download into {zips}")
    if not todo:
        return
    script = (
        'out="zips/$0.zip"; [ -s "$out" ] && exit 0; '
        f'curl -sS --max-time 60 --retry 2 --retry-delay 1 -A "{USER_AGENT}" '
        '-o "$out" "$1" || rm -f "$out"'
    )
    payload = "".join(f"{w} {u}\n" for w, u in todo)
    subprocess.run(["xargs", "-P", "6", "-n", "2", "bash", "-c", script],
                   input=payload, text=True, cwd=workdir, check=False)
    print(f"{len(list(zips.glob('*.zip')))} zips on disk")


def body(text: str) -> str:
    lines = text.split("\n")
    # The header is the title/author lines plus an optional ---- block
    # explaining the notation; the work starts after the second separator.
    separators = [i for i, l in enumerate(lines[:40]) if SEPARATOR.match(l.strip())]
    start = separators[1] + 1 if len(separators) >= 2 else 0
    end = len(lines)
    for i in range(len(lines) - 1, max(start, len(lines) - 400) - 1, -1):
        if lines[i].startswith("底本："):
            end = i
            break
    return "\n".join(lines[start:end])


def clean(text: str) -> str:
    text = ANNOTATION.sub("", text)
    text = RUBY.sub("", text)
    text = RUBY_MARK.sub("", text)
    text = text.replace("　", " ")
    return re.sub(r"\s+", " ", text).strip()


def extract(workdir: Path, out_path: Path) -> None:
    meta = {}
    for line in (workdir / "works.tsv").open():
        work_id, title, author, _url = line.rstrip("\n").split("\t")
        meta[work_id] = (title, author)

    kept = skipped = characters = 0
    with out_path.open("w", encoding="utf-8", newline="") as out:
        for path in sorted((workdir / "zips").glob("*.zip")):
            try:
                with zipfile.ZipFile(path) as z:
                    names = [n for n in z.namelist() if n.lower().endswith(".txt")]
                    if not names:
                        skipped += 1
                        continue
                    raw = z.read(names[0])
                text = raw.decode("shift_jis")
            except Exception:
                # A handful of the archives are damaged or use a private-use
                # character outside Shift_JIS; cp932 with replacement gets the
                # rest of the work rather than losing it.
                try:
                    text = raw.decode("cp932", errors="replace")
                except Exception:
                    skipped += 1
                    continue
            text = clean(body(text))
            if len(text) < 200:
                skipped += 1
                continue
            title, author = meta.get(path.stem, ("", ""))
            out.write(f"{path.stem}\t{title}\t{author}\t{text}\n")
            kept += 1
            characters += len(text)
    print(f"{kept} works, {skipped} skipped, {characters / 1e6:.1f}M characters "
          f"-> {out_path}")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    command = sys.argv[1]
    if command == "fetch":
        fetch(Path(sys.argv[2]))
    elif command == "extract":
        extract(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        print(__doc__)
        sys.exit(1)


main()
