//! Vilo: searches local worktrees straight from disk, the way ripgrep and
//! VS Code do, instead of waiting for the worktree scan to finish. On large
//! folders that scan takes minutes, while walking and filtering takes seconds.

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use collections::HashSet;
use ignore::{WalkBuilder, WalkState};
use project::{WorktreeId, search::SearchQuery};
use util::paths::{PathMatcher, PathStyle};
use util::rel_path::RelPath;

/// Directories skipped like VS Code's default search.exclude (see also worktree's ignore rules).
/// `.codex` and `.worktrees` hold agent checkouts of the same repos, so they only add duplicate hits.
const SKIPPED_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "bower_components",
    ".codex",
    ".worktrees",
];
/// Large enough for generated sources; bigger files are almost always data or bundles.
const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

pub(crate) struct DiskRoot {
    pub worktree_id: WorktreeId,
    pub abs_path: Arc<Path>,
    /// This worktree's `file_scan_exclusions`, so search skips what the project never scans.
    pub scan_exclusions: PathMatcher,
}

pub(crate) struct DiskCandidate {
    pub worktree_id: WorktreeId,
    pub rel_path: Arc<RelPath>,
    /// File contents with line endings normalized, as a buffer would hold them.
    pub text: String,
    /// Lets a later single replace confirm the file still holds the searched text.
    pub text_hash: u64,
}

#[derive(Default)]
pub(crate) struct WalkProgress {
    pub files_searched: AtomicUsize,
    pub cancelled: AtomicBool,
}

/// Walks `roots` on dedicated threads and sends every file whose contents match `query`.
/// The returned receiver closes when the walk ends or `progress.cancelled` is set.
pub(crate) fn walk(
    roots: Vec<DiskRoot>,
    query: SearchQuery,
    path_style: PathStyle,
    skip_abs_paths: HashSet<PathBuf>,
    progress: Arc<WalkProgress>,
) -> smol::channel::Receiver<DiskCandidate> {
    // Each queued candidate holds a whole file, so keep the queue short.
    let (tx, rx) = smol::channel::bounded(16);
    // Walk only outermost roots; files under nested worktrees still map to the deepest root.
    let mut walk_roots = roots.iter().filter(|root| {
        !roots.iter().any(|other| {
            other.abs_path != root.abs_path && root.abs_path.starts_with(&other.abs_path)
        })
    });
    let Some(first) = walk_roots.next() else {
        return rx;
    };
    let mut builder = WalkBuilder::new(&first.abs_path);
    for root in walk_roots {
        builder.add(&root.abs_path);
    }
    builder
        .hidden(false)
        .ignore(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        // Honor .gitignore files even in folders that are not git repositories.
        .require_git(false)
        .follow_links(false)
        .filter_entry(|entry| {
            !(entry.file_type().is_some_and(|kind| kind.is_dir())
                && SKIPPED_DIRS.iter().any(|dir| entry.file_name() == *dir))
        });
    let walker = builder.build_parallel();
    let roots = Arc::new(roots);
    let query = Arc::new(query);
    let skip_abs_paths = Arc::new(skip_abs_paths);

    std::thread::Builder::new()
        .name("vilo-disk-search".into())
        .spawn(move || {
            walker.run(|| {
                let tx = tx.clone();
                let roots = roots.clone();
                let query = query.clone();
                let skip_abs_paths = skip_abs_paths.clone();
                let progress = progress.clone();
                Box::new(move |entry| {
                    // A closed channel means the panel or its search is gone.
                    if progress.cancelled.load(Ordering::Relaxed) || tx.is_closed() {
                        return WalkState::Quit;
                    }
                    let Ok(entry) = entry else {
                        return WalkState::Continue;
                    };
                    let Some(kind) = entry.file_type() else {
                        return WalkState::Continue;
                    };
                    let abs_path = entry.path();
                    if kind.is_dir() {
                        // Prune excluded directories instead of visiting every file inside them.
                        let excluded = relative_to_root(&roots, abs_path, path_style).is_some_and(
                            |(root, rel_path)| {
                                !rel_path.is_empty()
                                    && (root.scan_exclusions.is_match(rel_path.as_ref())
                                        || query.files_to_exclude().is_match(rel_path.as_ref()))
                            },
                        );
                        return if excluded {
                            WalkState::Skip
                        } else {
                            WalkState::Continue
                        };
                    }
                    // Search symlinked files, but never follow symlinked directories (loops).
                    let is_file = kind.is_file()
                        || (kind.is_symlink()
                            && fs::metadata(abs_path).is_ok_and(|metadata| metadata.is_file()));
                    if !is_file || skip_abs_paths.contains(abs_path) {
                        return WalkState::Continue;
                    }
                    let Some((root, rel_path)) = relative_to_root(&roots, abs_path, path_style)
                    else {
                        return WalkState::Continue;
                    };
                    if root.scan_exclusions.is_match(rel_path.as_ref())
                        || !query.match_path(&rel_path)
                    {
                        return WalkState::Continue;
                    }
                    progress.files_searched.fetch_add(1, Ordering::Relaxed);
                    let Some(bytes) = read_bytes(abs_path) else {
                        return WalkState::Continue;
                    };
                    // Match on the raw bytes first so only matching files pay for decoding. UTF-16
                    // text and multi-line queries (normalized to \n) must be matched after decoding.
                    let decode_first = is_utf16(&bytes) || query.as_str().contains('\n');
                    // Skip a UTF-8 BOM so anchored patterns like `^foo` still match the first line.
                    let without_bom = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&bytes);
                    if !decode_first && !detects(&query, without_bom) {
                        return WalkState::Continue;
                    }
                    let text = decode(&bytes);
                    if decode_first && !detects(&query, text.as_bytes()) {
                        return WalkState::Continue;
                    }
                    let candidate = DiskCandidate {
                        worktree_id: root.worktree_id,
                        rel_path,
                        text_hash: text_hash(&text),
                        text,
                    };
                    if tx.send_blocking(candidate).is_err() {
                        return WalkState::Quit;
                    }
                    WalkState::Continue
                })
            });
        })
        .ok();
    rx
}

fn relative_to_root<'a>(
    roots: &'a [DiskRoot],
    abs_path: &Path,
    path_style: PathStyle,
) -> Option<(&'a DiskRoot, Arc<RelPath>)> {
    // Prefer the deepest root when worktrees are nested.
    let root = roots
        .iter()
        .filter(|root| abs_path.starts_with(&root.abs_path))
        .max_by_key(|root| root.abs_path.as_os_str().len())?;
    let relative = abs_path.strip_prefix(&root.abs_path).ok()?;
    let rel_path = RelPath::new(relative, path_style).ok()?.into_arc();
    Some((root, rel_path))
}

fn is_utf16(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xFF, 0xFE]) || bytes.starts_with(&[0xFE, 0xFF])
}

fn detects(query: &SearchQuery, mut bytes: &[u8]) -> bool {
    matches!(
        futures::executor::block_on(query.detect(&mut bytes)),
        Ok(Some(_))
    )
}

/// Reads a file, skipping oversized and binary ones. UTF-16 files are kept
/// rather than mistaken for binary because of their zero bytes.
fn read_bytes(abs_path: &Path) -> Option<Vec<u8>> {
    let mut file = fs::File::open(abs_path).ok()?;
    if file.metadata().ok()?.len() > MAX_FILE_BYTES {
        return None;
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    if !is_utf16(&bytes) && bytes[..bytes.len().min(BINARY_SNIFF_BYTES)].contains(&0) {
        return None;
    }
    Some(bytes)
}

/// Decodes file bytes and normalizes line endings, as a buffer would hold them.
pub(crate) fn text_hash(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

fn decode(bytes: &[u8]) -> String {
    let mut text = match bytes {
        [0xFF, 0xFE, rest @ ..] => decode_utf16(rest, u16::from_le_bytes),
        [0xFE, 0xFF, rest @ ..] => decode_utf16(rest, u16::from_be_bytes),
        // A UTF-8 BOM is not part of the text; keeping it would shift first-line columns.
        _ => String::from_utf8_lossy(bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes))
            .into_owned(),
    };
    text::LineEnding::normalize(&mut text);
    text
}

fn decode_utf16(bytes: &[u8], to_unit: fn([u8; 2]) -> u16) -> String {
    let units = bytes
        .chunks_exact(2)
        .map(|pair| to_unit([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    String::from_utf16_lossy(&units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use project::search::SearchQuery;

    fn query(text: &str) -> SearchQuery {
        SearchQuery::text(
            text,
            false,
            false,
            false,
            PathMatcher::default(),
            PathMatcher::default(),
            false,
            None,
        )
        .unwrap()
    }

    #[test]
    fn test_walk_respects_ignores_outside_git() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Not a git repository: .gitignore must still apply, as ripgrep's --no-require-git does.
        fs::write(root.join(".gitignore"), "dist/\n").unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("dist")).unwrap();
        fs::create_dir_all(root.join("node_modules/pkg")).unwrap();
        fs::create_dir_all(root.join(".github")).unwrap();
        fs::write(root.join("src/a.ts"), "SendSmsCode()\r\n").unwrap();
        fs::write(root.join("dist/a.js"), "SendSmsCode").unwrap();
        fs::write(root.join("node_modules/pkg/i.js"), "SendSmsCode").unwrap();
        fs::write(root.join(".github/ci.yml"), "SendSmsCode").unwrap();
        fs::write(root.join("bin.dat"), b"SendSmsCode\0\0").unwrap();
        fs::write(root.join("other.ts"), "nothing here").unwrap();

        let progress = Arc::new(WalkProgress::default());
        let rx = walk(
            vec![DiskRoot {
                worktree_id: WorktreeId::from_usize(1),
                abs_path: root.into(),
                scan_exclusions: PathMatcher::default(),
            }],
            query("SendSmsCode"),
            PathStyle::local(),
            HashSet::default(),
            progress.clone(),
        );
        let mut found = Vec::new();
        // The channel closes once the walk finishes and drops its senders.
        while let Ok(candidate) = smol::block_on(rx.recv()) {
            assert!(
                !candidate.text.contains('\r'),
                "line endings are normalized"
            );
            found.push(candidate.rel_path.as_unix_str().to_string());
        }
        found.sort();
        assert_eq!(found, vec![".github/ci.yml", "src/a.ts"]);
        assert!(progress.files_searched.load(Ordering::Relaxed) >= 4);
    }

    #[test]
    fn test_walk_nested_roots_bom_symlinks_and_exclusions() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("nested/src")).unwrap();
        fs::create_dir_all(root.join("copies")).unwrap();
        fs::write(root.join("outer.ts"), b"\xEF\xBB\xBFSendSmsCode").unwrap();
        fs::write(root.join("nested/src/inner.ts"), "SendSmsCode").unwrap();
        fs::write(root.join("copies/old.ts"), "SendSmsCode").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("nested/src/inner.ts"), root.join("link.ts")).unwrap();

        let outer = WorktreeId::from_usize(1);
        let nested = WorktreeId::from_usize(2);
        let rx = walk(
            vec![
                DiskRoot {
                    worktree_id: outer,
                    abs_path: root.into(),
                    scan_exclusions: PathMatcher::new(["copies"], PathStyle::local()).unwrap(),
                },
                DiskRoot {
                    worktree_id: nested,
                    abs_path: root.join("nested").into(),
                    scan_exclusions: PathMatcher::default(),
                },
            ],
            query("SendSmsCode"),
            PathStyle::local(),
            HashSet::default(),
            Arc::new(WalkProgress::default()),
        );
        let mut found = Vec::new();
        while let Ok(candidate) = smol::block_on(rx.recv()) {
            if candidate.rel_path.as_unix_str() == "outer.ts" {
                assert_eq!(candidate.text, "SendSmsCode", "UTF-8 BOM is stripped");
            }
            found.push((
                candidate.worktree_id,
                candidate.rel_path.as_unix_str().to_string(),
            ));
        }
        found.sort();
        let mut expected = vec![
            (outer, "outer.ts".to_string()),
            (nested, "src/inner.ts".to_string()),
        ];
        #[cfg(unix)]
        expected.push((outer, "link.ts".to_string()));
        expected.sort();
        // The nested file appears once, under its own worktree; "copies" is excluded for the outer root.
        assert_eq!(found, expected);
    }

    #[test]
    fn test_walk_anchored_regex_matches_after_bom() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("bom.ts"), b"\xEF\xBB\xBFSendSmsCode()").unwrap();
        let query = SearchQuery::regex(
            "^SendSmsCode",
            false,
            true,
            false,
            false,
            PathMatcher::default(),
            PathMatcher::default(),
            false,
            None,
        )
        .unwrap();
        let rx = walk(
            vec![DiskRoot {
                worktree_id: WorktreeId::from_usize(1),
                abs_path: dir.path().into(),
                scan_exclusions: PathMatcher::default(),
            }],
            query,
            PathStyle::local(),
            HashSet::default(),
            Arc::new(WalkProgress::default()),
        );
        let candidate = smol::block_on(rx.recv()).expect("BOM file matches ^SendSmsCode");
        assert_eq!(candidate.text, "SendSmsCode()");
    }

    /// Manual benchmark: `VILO_BENCH_DIR=~/Desktop/Projects cargo test -p search --lib bench_walk -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_walk() {
        let Ok(dir) = std::env::var("VILO_BENCH_DIR") else {
            return;
        };
        let query = SearchQuery::text(
            "SendSmsCode",
            true,
            true,
            false,
            PathMatcher::default(),
            PathMatcher::new(
                std::env::var("VILO_BENCH_EXCLUDE")
                    .unwrap_or_default()
                    .split(',')
                    .filter(|glob| !glob.is_empty()),
                PathStyle::local(),
            )
            .unwrap(),
            false,
            None,
        )
        .unwrap();
        let progress = Arc::new(WalkProgress::default());
        let started = std::time::Instant::now();
        let rx = walk(
            vec![DiskRoot {
                worktree_id: WorktreeId::from_usize(1),
                abs_path: Path::new(&dir).into(),
                scan_exclusions: PathMatcher::default(),
            }],
            query,
            PathStyle::local(),
            HashSet::default(),
            progress.clone(),
        );
        let mut matched = 0;
        let mut first = None;
        while smol::block_on(rx.recv()).is_ok() {
            first.get_or_insert_with(|| started.elapsed());
            matched += 1;
        }
        eprintln!(
            "walk: {:?} total, first match {:?}, {} files searched, {} matched",
            started.elapsed(),
            first,
            progress.files_searched.load(Ordering::Relaxed),
            matched
        );
    }
}
