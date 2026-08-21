// Tantivy side of the cpp-searchlib comparison.
//
// Deliberately fed a corpus that has already been segmented into
// space-separated terms by cpp-searchlib's own splitter, and indexed with a
// whitespace tokenizer, so that both engines index exactly the same term
// sequence. Otherwise the comparison would be measuring Lindera against
// cpp-segmentlib rather than the two search engines.
//
// Reports the same three things the C++ harness does: build time, index size
// on disk, and best-of-N top-10 latency per query.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{TextAnalyzer, WhitespaceTokenizer};
use tantivy::{Index, IndexWriter, ReloadPolicy};

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += if meta.is_dir() {
                    dir_size(&entry.path())
                } else {
                    meta.len()
                };
            }
        }
    }
    total
}

/// Best of `runs`, matching test_utils.h's estimator on the C++ side: the
/// fastest observed run is the one least polluted by scheduling noise.
fn best_of<F: FnMut()>(runs: usize, mut f: F) -> f64 {
    f(); // warm up
    let mut best = f64::MAX;
    for _ in 0..runs {
        let start = Instant::now();
        f();
        let us = start.elapsed().as_secs_f64() * 1e6;
        if us < best {
            best = us;
        }
    }
    best
}

fn main() -> tantivy::Result<()> {
    let mut corpus = String::new();
    let mut queries_path = String::new();
    let mut index_dir = String::from("/tmp/tantivy-bench-index");
    let mut text_field_index = 3usize;
    let mut runs = 20usize;
    let mut heap_mb = 512usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        let take = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--corpus" => corpus = take(&mut i),
            "--queries" => queries_path = take(&mut i),
            "--index-dir" => index_dir = take(&mut i),
            "--text-field" => text_field_index = take(&mut i).parse().unwrap(),
            "--runs" => runs = take(&mut i).parse().unwrap(),
            "--heap-mb" => heap_mb = take(&mut i).parse().unwrap(),
            other => {
                eprintln!("unknown option {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Not stored, only indexed: cpp-searchlib keeps no copy of the document
    // text either (and none of its byte offsets under TextRangeStorage::Skip),
    // so storing here would compare an index against an index plus a document
    // store. Positions are on because the query set contains a phrase.
    let mut schema_builder = Schema::builder();
    let indexing = TextFieldIndexing::default()
        .set_tokenizer("ws")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default().set_indexing_options(indexing);
    let text = schema_builder.add_text_field("text", text_options);
    let schema = schema_builder.build();

    let _ = fs::remove_dir_all(&index_dir);
    fs::create_dir_all(&index_dir).unwrap();
    let index = Index::create_in_dir(&index_dir, schema.clone())?;
    index.tokenizers().register(
        "ws",
        TextAnalyzer::builder(WhitespaceTokenizer::default()).build(),
    );

    let file = fs::File::open(&corpus).unwrap_or_else(|e| {
        eprintln!("cannot open {corpus}: {e}");
        std::process::exit(1);
    });

    let mut documents = 0usize;
    let mut bytes = 0usize;
    let build_start = Instant::now();
    {
        let mut writer: IndexWriter = index.writer(heap_mb * 1024 * 1024)?;
        for line in BufReader::new(file).lines() {
            let line = line.unwrap();
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() <= text_field_index {
                continue;
            }
            let body = fields[text_field_index];
            bytes += body.len();
            documents += 1;
            let mut doc = tantivy::TantivyDocument::default();
            doc.add_text(text, body);
            writer.add_document(doc)?;
        }
        writer.commit()?;
    }
    let build_ms = build_start.elapsed().as_secs_f64() * 1e3;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    println!("corpus     {corpus}");
    println!("documents  {documents}");
    println!("text       {:.1} MiB", bytes as f64 / 1048576.0);
    println!("build      {build_ms:.1} ms");
    println!(
        "index dir  {:.1} MiB",
        dir_size(Path::new(&index_dir)) as f64 / 1048576.0
    );
    println!("segments   {}", searcher.segment_readers().len());

    let mut query_parser = QueryParser::for_index(&index, vec![text]);
    // cpp-searchlib's grammar makes a space an And and `|` an Or. Tantivy
    // defaults a space to Or and spells Or as a keyword, so both are aligned
    // here rather than by keeping two query files: one file that both engines
    // read is what makes the hit counts checkable against each other, and a
    // hit-count mismatch is the only signal that they are not answering the
    // same question.
    query_parser.set_conjunction_by_default();

    println!();
    println!(
        "{:<24} {:>10} {:>12}",
        "query", "hits", "top10_us"
    );

    let queries_file = fs::File::open(&queries_path).unwrap_or_else(|e| {
        eprintln!("cannot open {queries_path}: {e}");
        std::process::exit(1);
    });
    for line in BufReader::new(queries_file).lines() {
        let line = line.unwrap();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, text_query) = match line.split_once('\t') {
            Some((n, q)) => (n.to_string(), q.to_string()),
            None => (line.clone(), line.clone()),
        };

        let translated = text_query.replace(" | ", " OR ");
        let query = match query_parser.parse_query(&translated) {
            Ok(q) => q,
            Err(e) => {
                println!("{name:<24}  PARSE FAILED ({e})");
                continue;
            }
        };

        // Total matching documents, for comparison with the C++ hit column.
        let hits = searcher.search(&query, &tantivy::collector::Count)?;

        let top10_us = best_of(runs, || {
            let _ = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        });

        println!("{name:<24} {hits:>10} {top10_us:>12.2}");
    }

    Ok(())
}
