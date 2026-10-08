//! Spilling what a processing component holds for its declared target states
//! out of memory.
//!
//! A component holds each declared target-state value from its declaration
//! until its pre-commit has reconciled it, and each action `reconcile`
//! returned until its sink has applied it. Past a bound on what it keeps in
//! memory ([`TargetStateSpillSettings::threshold_bytes`]), the rest goes to an
//! anonymous temporary file in the state store's directory, in declaration
//! order, and is read back in chunks of at most
//! [`TargetStateSpillSettings::chunk_bytes`] when it is needed: the values for
//! `reconcile`, the actions for the sinks. A spill file lives as long as what
//! it holds would have lived in memory — the values until pre-commit is done,
//! the actions until the sinks have applied them — and goes with the component
//! on any failure.
//!
//! What can spill is up to the profile: see [`Spillable`].

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::prelude::*;

/// Default for [`TargetStateSpillSettings::threshold_bytes`].
pub const DEFAULT_SPILL_THRESHOLD_BYTES: usize = 32 << 20;

/// Bounds of [`TargetStateSpillSettings::chunk_bytes`]: the threshold itself
/// when it is in range, so a threshold set to zero (everything spills) still
/// reads back in reasonably sized chunks, and a large one still hands the
/// sinks bounded batches.
const MIN_CHUNK_BYTES: usize = 1 << 20;
const MAX_CHUNK_BYTES: usize = 8 << 20;

/// Size of the write buffer in front of a spill file.
const WRITE_BUFFER_BYTES: usize = 1 << 20;

/// A declared target-state value or a target action the engine can move out of
/// memory and back.
pub trait Spillable: Sized {
    /// Bytes this item holds in memory, as far as the profile can tell. Drives
    /// when a component starts spilling; an item the profile cannot size (and
    /// typically cannot spill either) reports 0.
    fn resident_size(&self) -> usize;

    /// The bytes to spill this item as, or `None` to keep it in memory.
    fn to_spill_bytes(&self) -> Result<Option<Cow<'_, [u8]>>>;

    /// An item equal to the one spilled as `bytes`. The slice is borrowed from
    /// a read buffer, so the item must own what it keeps of it.
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self>;
}

/// How a processing component spills its declared target states.
#[derive(Clone, Debug)]
pub struct TargetStateSpillSettings {
    /// Bytes of declared target-state values a component keeps in memory
    /// before spilling the ones declared after; likewise for the actions its
    /// pre-commit produces.
    pub threshold_bytes: usize,
    /// Bytes of items — spilled and resident alike — handed back at a time
    /// once a component has spilled: a bound on what a sink call carries, and
    /// on what a read from a spill file brings into memory.
    pub chunk_bytes: usize,
    /// Directory the spill files are created in.
    pub dir: PathBuf,
}

impl TargetStateSpillSettings {
    pub fn new(threshold_bytes: usize, dir: PathBuf) -> Self {
        Self {
            threshold_bytes,
            chunk_bytes: threshold_bytes.clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES),
            dir,
        }
    }
}

/// Where a spilled item's bytes are in its spill file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SpillRef {
    pub offset: u64,
    pub len: usize,
}

impl SpillRef {
    fn end(&self) -> u64 {
        self.offset + self.len as u64
    }
}

/// Tracks what a component holds in memory of one kind of spillable item and
/// decides whether the next one stays resident. Once the resident bytes reach
/// the threshold, every later item spills (when it can), so the resident ones
/// are a prefix of the declaration order.
pub(crate) struct SpillBudget {
    resident_bytes: usize,
    threshold_bytes: usize,
}

impl SpillBudget {
    pub fn new(threshold_bytes: usize) -> Self {
        Self {
            resident_bytes: 0,
            threshold_bytes,
        }
    }

    /// Whether an item of `size` bytes stays resident, accounting for it if so.
    pub fn admit(&mut self, size: usize) -> bool {
        if self.resident_bytes >= self.threshold_bytes {
            return false;
        }
        self.resident_bytes += size;
        true
    }
}

/// An anonymous temporary file being appended to. Deleted when its last handle
/// closes, so nothing outlives the component it belongs to.
pub(crate) struct SpillWriter {
    writer: BufWriter<File>,
    len: u64,
}

impl SpillWriter {
    pub fn new(dir: &Path) -> Result<Self> {
        let file = tempfile::tempfile_in(dir).map_err(|e| {
            internal_error!(
                "Failed to create a spill file for target states in {}: {e}",
                dir.display()
            )
        })?;
        Ok(Self {
            writer: BufWriter::with_capacity(WRITE_BUFFER_BYTES, file),
            len: 0,
        })
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<SpillRef> {
        let offset = self.len;
        self.writer.write_all(bytes)?;
        self.len += bytes.len() as u64;
        Ok(SpillRef {
            offset,
            len: bytes.len(),
        })
    }

    /// Flush, and turn into the reader of what was written.
    pub fn finish(self) -> Result<SpillReader> {
        let file = self
            .writer
            .into_inner()
            .map_err(|e| internal_error!("Failed to flush a spill file: {}", e.error()))?;
        Ok(SpillReader {
            file: Arc::new(file),
        })
    }
}

/// Reads back what a [`SpillWriter`] wrote. Cloning shares the file.
#[derive(Clone)]
pub(crate) struct SpillReader {
    file: Arc<File>,
}

impl SpillReader {
    /// The bytes in `[start, end)`.
    pub fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; (end - start) as usize];
        read_exact_at(&self.file, &mut buf, start)?;
        Ok(buf)
    }

    /// Like [`Self::read_range`], off the async runtime's workers.
    pub async fn read_range_blocking(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let reader = self.clone();
        tokio::task::spawn_blocking(move || reader.read_range(start, end)).await?
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "spill file ended early",
            ));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// A run of consecutive items whose spilled bytes are read back together: the
/// items' index range, and the file range their spilled refs cover.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SpillChunkPlan {
    pub items: std::ops::Range<usize>,
    /// `None` when no item of the run is spilled.
    pub range: Option<(u64, u64)>,
}

/// Split items, given as their size in bytes and where they are spilled (the
/// spilled ones at increasing offsets), into runs of at most `chunk_bytes`
/// each. A run always holds at least one item, so a single item larger than
/// that is a run of its own.
pub(crate) fn plan_chunks(
    items: impl ExactSizeIterator<Item = (usize, Option<SpillRef>)>,
    chunk_bytes: usize,
) -> Vec<SpillChunkPlan> {
    let total = items.len();
    let mut plans = Vec::new();
    let mut start = 0;
    let mut range: Option<(u64, u64)> = None;
    let mut bytes = 0usize;
    for (idx, (size, spilled)) in items.enumerate() {
        if idx > start && bytes.saturating_add(size) > chunk_bytes {
            plans.push(SpillChunkPlan {
                items: start..idx,
                range,
            });
            start = idx;
            range = None;
            bytes = 0;
        }
        if let Some(at) = spilled {
            range = Some(match range {
                Some((first, _)) => (first, at.end()),
                None => (at.offset, at.end()),
            });
        }
        bytes += size;
    }
    if start < total {
        plans.push(SpillChunkPlan {
            items: start..total,
            range,
        });
    }
    plans
}

/// One run's spilled bytes, read on first use. Items are handed out as slices
/// into the buffer, so what a profile keeps of a decoded item is its own copy.
pub(crate) struct SpillChunk<'a> {
    reader: &'a SpillReader,
    range: Option<(u64, u64)>,
    buf: Option<Vec<u8>>,
}

impl<'a> SpillChunk<'a> {
    pub fn new(reader: &'a SpillReader, range: Option<(u64, u64)>) -> Self {
        Self {
            reader,
            range,
            buf: None,
        }
    }

    /// A chunk whose bytes were read ahead of time (see
    /// [`SpillReader::read_range_blocking`]).
    pub fn with_bytes(reader: &'a SpillReader, range: Option<(u64, u64)>, buf: Vec<u8>) -> Self {
        Self {
            reader,
            range,
            buf: Some(buf),
        }
    }

    pub fn get(&mut self, at: SpillRef) -> Result<&[u8]> {
        let (start, end) = self.range.ok_or_else(|| {
            internal_error!("spilled item in a run planned without spilled bytes")
        })?;
        if at.offset < start || at.end() > end {
            internal_bail!("spilled item {at:?} is outside its run's range {start}..{end}");
        }
        if self.buf.is_none() {
            self.buf = Some(self.reader.read_range(start, end)?);
        }
        let buf = self.buf.as_ref().expect("read just above");
        let from = (at.offset - start) as usize;
        Ok(&buf[from..from + at.len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_items(items: &[&[u8]]) -> (SpillReader, Vec<SpillRef>) {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SpillWriter::new(dir.path()).unwrap();
        let refs: Vec<SpillRef> = items
            .iter()
            .map(|item| writer.append(item).unwrap())
            .collect();
        // The file is anonymous: the directory can go while it is still read.
        let reader = writer.finish().unwrap();
        drop(dir);
        (reader, refs)
    }

    #[test]
    fn items_read_back_through_chunks() {
        // Every third item from the second stays resident: it counts toward
        // the chunk like the spilled ones, without a spill ref.
        let items: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 3 + i as usize]).collect();
        let (reader, refs) = write_items(&items.iter().map(Vec::as_slice).collect::<Vec<_>>());
        let spilled: Vec<Option<SpillRef>> = refs
            .iter()
            .enumerate()
            .map(|(i, r)| (i % 3 != 1).then_some(*r))
            .collect();
        let plans = plan_chunks(
            items
                .iter()
                .zip(&spilled)
                .map(|(item, at)| (item.len(), *at)),
            20,
        );
        assert_eq!(
            plans.iter().map(|p| p.items.clone()).collect::<Vec<_>>(),
            vec![0..4, 4..6, 6..8, 8..9, 9..10]
        );
        for plan in &plans {
            let mut chunk = SpillChunk::new(&reader, plan.range);
            for idx in plan.items.clone() {
                if let Some(at) = spilled[idx] {
                    assert_eq!(chunk.get(at).unwrap(), items[idx].as_slice());
                }
            }
        }
    }

    #[test]
    fn a_run_holds_an_oversized_item_on_its_own() {
        let (reader, refs) = write_items(&[b"ab", b"0123456789", b"cd"]);
        let plans = plan_chunks(refs.iter().map(|r| (r.len, Some(*r))), 4);
        assert_eq!(
            plans,
            vec![
                SpillChunkPlan {
                    items: 0..1,
                    range: Some((0, 2))
                },
                SpillChunkPlan {
                    items: 1..2,
                    range: Some((2, 12))
                },
                SpillChunkPlan {
                    items: 2..3,
                    range: Some((12, 14))
                },
            ]
        );
        let mut chunk = SpillChunk::new(&reader, plans[1].range);
        assert_eq!(chunk.get(refs[1]).unwrap(), b"0123456789");
        assert!(chunk.get(refs[0]).is_err(), "outside the run");
    }

    #[test]
    fn resident_items_make_runs_without_a_range() {
        assert_eq!(
            plan_chunks([(5, None), (5, None)].into_iter(), 10),
            vec![SpillChunkPlan {
                items: 0..2,
                range: None
            }]
        );
        assert_eq!(
            plan_chunks([(6, None), (6, None)].into_iter(), 10),
            vec![
                SpillChunkPlan {
                    items: 0..1,
                    range: None
                },
                SpillChunkPlan {
                    items: 1..2,
                    range: None
                },
            ]
        );
        assert!(plan_chunks(std::iter::empty(), 10).is_empty());
    }

    #[test]
    fn chunk_bytes_follow_the_threshold_within_bounds() {
        let dir = PathBuf::from(".");
        assert_eq!(
            TargetStateSpillSettings::new(0, dir.clone()).chunk_bytes,
            MIN_CHUNK_BYTES
        );
        assert_eq!(
            TargetStateSpillSettings::new(2 << 20, dir.clone()).chunk_bytes,
            2 << 20
        );
        assert_eq!(
            TargetStateSpillSettings::new(DEFAULT_SPILL_THRESHOLD_BYTES, dir).chunk_bytes,
            MAX_CHUNK_BYTES
        );
    }

    #[test]
    fn budget_keeps_a_prefix_then_spills() {
        let mut budget = SpillBudget::new(10);
        assert!(budget.admit(6));
        assert!(budget.admit(6), "the item that crosses the threshold stays");
        assert!(!budget.admit(1), "every item after spills");
        assert!(!SpillBudget::new(0).admit(0), "a zero threshold spills all");
    }
}
