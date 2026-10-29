//! Rust versions of the 6.5840 word-count and inverted-index applications.

use crate::KeyValue;

fn words(text: &str) -> impl Iterator<Item = &str> {
    // Match the Go applications: a word is a maximal run of Unicode letters.
    let mut ranges = Vec::new();
    let mut start = None;
    for (index, ch) in text.char_indices() {
        if ch.is_alphabetic() {
            start.get_or_insert(index);
        } else if let Some(begin) = start.take() {
            ranges.push(&text[begin..index]);
        }
    }
    if let Some(begin) = start {
        ranges.push(&text[begin..]);
    }
    ranges.into_iter()
}

/// Map function for word count. The filename is intentionally ignored.
pub fn wc_map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    words(contents)
        .map(|word| KeyValue::new(word, "1"))
        .collect()
}

/// Reduce function for word count.
pub fn wc_reduce(_key: &str, values: &[String]) -> String {
    values.len().to_string()
}

/// Map function for the inverted index. Each document contributes at most
/// one value for each word.
pub fn indexer_map(document: &str, contents: &str) -> Vec<KeyValue> {
    let mut unique = std::collections::BTreeSet::new();
    for word in words(contents) {
        unique.insert(word);
    }
    unique
        .into_iter()
        .map(|word| KeyValue::new(word, document))
        .collect()
}

/// Reduce function for the inverted index.
pub fn indexer_reduce(_key: &str, values: &[String]) -> String {
    let mut documents = values.to_vec();
    documents.sort();
    format!("{} {}", documents.len(), documents.join(","))
}

/// Distributed grep: emit each matching input line as an intermediate record.
pub fn grep_map(_filename: &str, contents: &str, pattern: &str) -> Vec<KeyValue> {
    contents
        .lines()
        .filter(|line| line.contains(pattern))
        .map(|line| KeyValue::new(line, line))
        .collect()
}

/// Grep's reducer preserves every matching line supplied to it.
pub fn grep_reduce(_key: &str, values: &[String]) -> String {
    values.join("\n")
}

/// Map a web request log line to its requested URL and a unit count.
/// Accepts either a plain URL/path line or a common-log request containing
/// a quoted `METHOD URL HTTP/VERSION` request.
pub fn url_access_map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    contents
        .lines()
        .filter_map(|line| {
            let request = if let Some((_, after_quote)) = line.split_once('"') {
                after_quote.split('"').next().unwrap_or("")
            } else {
                line
            };
            let fields: Vec<_> = request.split_whitespace().collect();
            let url = if fields.len() >= 2 && fields[0].chars().all(|c| c.is_ascii_uppercase()) {
                fields[1]
            } else {
                fields.first().copied().unwrap_or("")
            };
            (!url.is_empty()).then(|| KeyValue::new(url, "1"))
        })
        .collect()
}

pub fn url_access_reduce(_url: &str, values: &[String]) -> String {
    values.len().to_string()
}

/// Emit `<target URL, source page>` for each HTTP(S) link in a page.
pub fn reverse_links_map(source: &str, contents: &str) -> Vec<KeyValue> {
    contents
        .split_whitespace()
        .filter_map(|token| {
            let start = token.find("http://").or_else(|| token.find("https://"))?;
            let target = token[start..].trim_matches(|c: char| {
                matches!(
                    c,
                    '<' | '>' | '"' | '\'' | '(' | ')' | '[' | ']' | ',' | ';' | '.'
                )
            });
            (target.starts_with("http://") || target.starts_with("https://"))
                .then(|| KeyValue::new(target, source))
        })
        .collect()
}

pub fn reverse_links_reduce(_target: &str, values: &[String]) -> String {
    let mut sources = values.to_vec();
    sources.sort();
    sources.dedup();
    sources.join(",")
}

/// Build one serialized term vector (`word=count,...`) per document, keyed
/// by the hostname in its URL.
pub fn term_vector_map(document_url: &str, contents: &str) -> Vec<KeyValue> {
    let Some(host) = url_host(document_url) else {
        return Vec::new();
    };
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for word in words(contents) {
        *counts.entry(word).or_default() += 1;
    }
    let vector = counts
        .into_iter()
        .map(|(word, count)| format!("{word}={count}"))
        .collect::<Vec<_>>()
        .join(",");
    vec![KeyValue::new(host, vector)]
}

/// Terms occurring fewer than two times across a host are discarded.
pub const TERM_VECTOR_MIN_COUNT: usize = 2;

pub fn term_vector_reduce(_host: &str, values: &[String]) -> String {
    let mut totals = std::collections::BTreeMap::<String, usize>::new();
    for value in values {
        for item in value.split(',').filter(|item| !item.is_empty()) {
            if let Some((word, count)) = item.rsplit_once('=') {
                if let Ok(count) = count.parse::<usize>() {
                    *totals.entry(word.to_owned()).or_default() += count;
                }
            }
        }
    }
    totals
        .into_iter()
        .filter(|(_, count)| *count >= TERM_VECTOR_MIN_COUNT)
        .map(|(word, count)| format!("{word}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Emit unique document IDs for each word (the inverted-index variant).
pub fn inverted_index_map(document_id: &str, contents: &str) -> Vec<KeyValue> {
    let unique: std::collections::BTreeSet<_> = words(contents).collect();
    unique
        .into_iter()
        .map(|word| KeyValue::new(word, document_id))
        .collect()
}

pub fn inverted_index_reduce(_word: &str, values: &[String]) -> String {
    let mut documents = values.to_vec();
    documents.sort();
    documents.dedup();
    documents.join(",")
}

/// Extract the first whitespace-delimited field as the sort key and preserve
/// the full source record as the value.
pub fn distributed_sort_map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    contents
        .lines()
        .filter_map(|record| {
            let key = record.split_whitespace().next()?;
            Some(KeyValue::new(key, record))
        })
        .collect()
}

/// Identity reduction for records sharing a key. Records are kept separate
/// with newlines so no input record is lost.
pub fn distributed_sort_reduce(_key: &str, values: &[String]) -> String {
    values.join("\n")
}

fn url_host(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.rsplit_once('@').map(|(_, host)| host))?;
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    (!host.is_empty()).then_some(host)
}

/// Dispatch a map application by its test/runtime name. Grep applications
/// use `grep:PATTERN`; all other names are listed in the Rust tests.
pub fn map(app: &str, filename: &str, contents: &str) -> Result<Vec<KeyValue>, String> {
    let result = match app {
        "wc" => wc_map(filename, contents),
        "indexer" => indexer_map(filename, contents),
        "url_access" => url_access_map(filename, contents),
        "reverse_links" => reverse_links_map(filename, contents),
        "term_vector" => term_vector_map(filename, contents),
        "inverted_index" => inverted_index_map(filename, contents),
        "distributed_sort" => distributed_sort_map(filename, contents),
        _ if app.starts_with("grep:") => grep_map(filename, contents, &app[5..]),
        _ => return Err(format!("unknown MapReduce application: {app}")),
    };
    Ok(result)
}

/// Dispatch a reduce application by the same runtime name used by [`map`].
pub fn reduce(app: &str, key: &str, values: &[String]) -> Result<String, String> {
    let result = match app {
        "wc" => wc_reduce(key, values),
        "indexer" => indexer_reduce(key, values),
        "url_access" => url_access_reduce(key, values),
        "reverse_links" => reverse_links_reduce(key, values),
        "term_vector" => term_vector_reduce(key, values),
        "inverted_index" => inverted_index_reduce(key, values),
        "distributed_sort" => distributed_sort_reduce(key, values),
        _ if app.starts_with("grep:") => grep_reduce(key, values),
        _ => return Err(format!("unknown MapReduce application: {app}")),
    };
    Ok(result)
}

/// Reject an unknown application even when the input or partition is empty.
pub fn validate_app(app: &str) -> Result<(), String> {
    map(app, "", "").map(|_| ())?;
    reduce(app, "", &[]).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_count_maps_and_reduces_words() {
        let mapped = wc_map("ignored.txt", "Rust, Rust! systems-世界");
        assert_eq!(
            mapped,
            vec![
                KeyValue::new("Rust", "1"),
                KeyValue::new("Rust", "1"),
                KeyValue::new("systems", "1"),
                KeyValue::new("世界", "1"),
            ]
        );
        assert_eq!(wc_reduce("Rust", &["1".into(), "1".into()]), "2");
    }

    #[test]
    fn indexer_emits_unique_words_and_sorted_documents() {
        let mapped = indexer_map("doc-a", "Rust rust Rust, systems");
        assert_eq!(
            mapped,
            vec![
                KeyValue::new("Rust", "doc-a"),
                KeyValue::new("rust", "doc-a"),
                KeyValue::new("systems", "doc-a"),
            ]
        );
        assert_eq!(
            indexer_reduce("Rust", &["doc-c".into(), "doc-a".into(), "doc-b".into()]),
            "3 doc-a,doc-b,doc-c",
        );
    }

    #[test]
    fn grep_keeps_only_matching_lines() {
        assert_eq!(
            grep_map("x", "Rust\nGo\nRust systems\n", "Rust"),
            vec![
                KeyValue::new("Rust", "Rust"),
                KeyValue::new("Rust systems", "Rust systems"),
            ]
        );
        assert_eq!(
            grep_reduce("Rust", &["Rust".into(), "Rust systems".into()]),
            "Rust\nRust systems"
        );
    }

    #[test]
    fn request_counts_group_identical_urls() {
        let mapped = url_access_map(
            "access.log",
            "127.0.0.1 - - [date] \"GET /a HTTP/1.1\" 200\nGET /b HTTP/1.1\n",
        );
        assert_eq!(
            mapped,
            vec![KeyValue::new("/a", "1"), KeyValue::new("/b", "1")]
        );
        assert_eq!(url_access_reduce("/a", &["1".into(), "1".into()]), "2");
    }

    #[test]
    fn reverse_links_groups_source_pages_by_target() {
        assert_eq!(
            reverse_links_map(
                "https://a.test",
                "<a href=\"https://b.test/p\"> https://c.test"
            ),
            vec![
                KeyValue::new("https://b.test/p", "https://a.test"),
                KeyValue::new("https://c.test", "https://a.test"),
            ]
        );
        assert_eq!(
            reverse_links_reduce("target", &["z".into(), "a".into()]),
            "a,z"
        );
    }

    #[test]
    fn host_term_vectors_sum_and_drop_rare_terms() {
        assert_eq!(
            term_vector_map("https://news.test/a", "rust rust systems")[0].key,
            "news.test"
        );
        assert_eq!(
            term_vector_reduce(
                "news.test",
                &["rust=2,systems=1".into(), "rust=1,systems=1".into()]
            ),
            "rust=3,systems=2"
        );
    }

    #[test]
    fn inverted_index_and_sort_preserve_records() {
        assert_eq!(
            inverted_index_reduce("rust", &["b".into(), "a".into(), "b".into()]),
            "a,b"
        );
        assert_eq!(
            distributed_sort_map("x", "b 2\na 1")[0],
            KeyValue::new("b", "b 2")
        );
        assert_eq!(
            distributed_sort_reduce("a", &["a 1".into(), "a 3".into()]),
            "a 1\na 3"
        );
    }
}
