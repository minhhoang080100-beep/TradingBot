//! Synchronous, single-writer durable checkpoints for account execution state.
//!
//! Records are length bounded, sequence numbered and linked by SHA-256 digests.
//! Opening an existing journal validates the entire chain and never repairs or
//! discards a damaged tail. A successful append has completed [`File::sync_all`].
//! An append error after writing begins is ambiguous: discard the poisoned
//! handle and reconcile exchange state before deciding what to do next.
//!
//! Keep the journal in an application-owned directory with restrictive access
//! permissions. The file lock excludes cooperating writers; it is not a defense
//! against a process that ignores locks or replaces files. Symlink checks are
//! best effort and do not eliminate path races in an untrusted directory. The
//! checksums detect accidental damage, not malicious edits. A complete suffix
//! removed exactly at a record boundary cannot be detected without an external
//! committed-sequence anchor, so recovery must always reconcile with the venue.
//!
//! On Unix, creation also syncs the parent directory. Windows uses the file's
//! `sync_all` guarantee; neither platform can promise durability against every
//! filesystem, storage-controller, or power-loss failure. These APIs perform
//! blocking filesystem work and should be called from `spawn_blocking` in async
//! applications. There is no automatic rotation, truncation, or compaction.

use std::{
    fmt,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::models::Exchange;

/// Largest checkpoint payload accepted by the journal, in bytes.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
/// Maximum journal size, including the header and all frame metadata.
pub const MAX_FILE_BYTES: u64 = 256 * 1024 * 1024;

const MAGIC: &[u8; 8] = b"ICTJRN01";
const VERSION: u32 = 1;
const MAX_IDENTITY_BYTES: usize = 1024;
const HEADER_PREFIX_BYTES: u64 = 16;
const DIGEST_BYTES: u64 = 32;
const FRAME_OVERHEAD_BYTES: u64 = 76;

/// Non-secret identity binding a journal to one venue, account and environment.
///
/// The account label is an operator-assigned identifier, never an API key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JournalIdentity {
    exchange: Exchange,
    account_label: String,
    demo: bool,
}

impl JournalIdentity {
    /// Validates a non-secret account label of 1–64 ASCII identifier characters.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::InvalidIdentity`] for a label containing anything
    /// except ASCII letters, digits, hyphens, underscores, or dots.
    pub fn new(
        exchange: Exchange,
        account_label: String,
        demo: bool,
    ) -> Result<Self, JournalError> {
        if account_label.is_empty()
            || account_label.len() > 64
            || !account_label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(JournalError::InvalidIdentity);
        }
        Ok(Self {
            exchange,
            account_label,
            demo,
        })
    }

    /// Returns the venue bound to this journal.
    #[must_use]
    pub const fn exchange(&self) -> Exchange {
        self.exchange
    }

    /// Returns the operator-assigned, non-secret account identifier.
    #[must_use]
    pub fn account_label(&self) -> &str {
        &self.account_label
    }

    /// Indicates whether the journal belongs to a demo account.
    #[must_use]
    pub const fn demo(&self) -> bool {
        self.demo
    }
}

impl<'de> Deserialize<'de> for JournalIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            exchange: Exchange,
            account_label: String,
            demo: bool,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.exchange, fields.account_label, fields.demo).map_err(de::Error::custom)
    }
}

/// A locked append-only journal; dropping it releases the operating-system lock.
///
/// Only the newest checkpoint is retained in memory. Dropping a poisoned handle
/// does not undo bytes already written to disk.
pub struct Journal {
    file: File,
    identity: JournalIdentity,
    sequence: u64,
    digest: [u8; 32],
    latest: Option<Vec<u8>>,
    byte_len: u64,
    poisoned: bool,
}

impl fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Journal")
            .field("identity", &self.identity)
            .field("sequence", &self.sequence)
            .field("byte_len", &self.byte_len)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl Journal {
    /// Creates a new journal without overwriting any existing filesystem entry.
    ///
    /// The parent directory must already exist and be trusted. If creation or
    /// synchronization fails, the possibly incomplete file is deliberately left
    /// in place for inspection; the next attempt must not overwrite it.
    ///
    /// # Errors
    ///
    /// Returns a sanitized path, lock, serialization, or filesystem error.
    pub fn create(path: impl AsRef<Path>, identity: JournalIdentity) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let parent = validate_parent(path)?;
        let encoded = serde_json::to_vec(&identity).map_err(|_| JournalError::Serialization)?;
        let length = u32::try_from(encoded.len()).map_err(|_| JournalError::InvalidIdentity)?;
        let mut header = Vec::with_capacity(16 + encoded.len());
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_be_bytes());
        header.extend_from_slice(&length.to_be_bytes());
        header.extend_from_slice(&encoded);
        let digest: [u8; 32] = Sha256::digest(&header).into();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(path)
            .map_err(|error| io_error("create", &error))?;
        lock_file(&file)?;
        check_regular_file(&file)?;
        file.write_all(&header)
            .and_then(|()| file.write_all(&digest))
            .and_then(|()| file.sync_all())
            .map_err(|error| io_error("initialize and sync", &error))?;
        sync_parent(&parent)?;
        Ok(Self {
            file,
            identity,
            sequence: 0,
            digest,
            latest: None,
            byte_len: HEADER_PREFIX_BYTES + u64::from(length) + DIGEST_BYTES,
            poisoned: false,
        })
    }

    /// Opens and exclusively locks an existing journal, validating every record.
    ///
    /// This method never creates a missing file and never truncates a torn tail.
    /// A fully written record can be recovered even if the previous process
    /// failed before receiving the append acknowledgement. Such records must
    /// not cause blind replay of exchange commands.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing, locked, oversized, damaged, unsupported,
    /// or differently scoped journal. Error values do not contain payloads.
    pub fn open(path: impl AsRef<Path>, expected: &JournalIdentity) -> Result<Self, JournalError> {
        let path = path.as_ref();
        validate_parent(path)?;
        let metadata = fs::symlink_metadata(path).map_err(|error| io_error("inspect", &error))?;
        if !metadata.file_type().is_file() {
            return Err(JournalError::UnsafePath);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error("open", &error))?;
        lock_file(&file)?;
        let byte_len = check_regular_file(&file)?;
        if byte_len > MAX_FILE_BYTES {
            return Err(JournalError::CapacityExceeded);
        }
        let (identity, digest, header_len) = read_header(&mut file)?;
        if &identity != expected {
            return Err(JournalError::IdentityMismatch);
        }
        let mut journal = Self {
            file,
            identity,
            sequence: 0,
            digest,
            latest: None,
            byte_len: header_len,
            poisoned: false,
        };
        while journal.byte_len < byte_len {
            journal.read_frame(byte_len)?;
        }
        if check_regular_file(&journal.file)? != byte_len {
            return Err(JournalError::Corrupt("file changed during recovery"));
        }
        Ok(journal)
    }

    /// Writes a checkpoint and synchronizes it before returning its sequence.
    ///
    /// Capacity and payload checks occur before any bytes are written. Any
    /// write, seek, or sync uncertainty poisons this handle: reopen and reconcile
    /// rather than retrying the exchange operation associated with a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Poisoned`] after an earlier uncertain write;
    /// otherwise reports invalid size, exhausted sequence/capacity, or I/O.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(JournalError::PayloadTooLarge);
        }
        let length = u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let next_len = self
            .byte_len
            .checked_add(FRAME_OVERHEAD_BYTES + u64::from(length))
            .filter(|size| *size <= MAX_FILE_BYTES)
            .ok_or(JournalError::CapacityExceeded)?;
        let digest = frame_digest(length, sequence, &self.digest, payload);
        // Allocate the new in-memory checkpoint before changing durable state.
        let checkpoint = payload.to_vec();
        self.poisoned = true;
        let actual_len = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|error| JournalError::DurabilityUncertain(error.kind()))?;
        if actual_len != self.byte_len {
            return Err(JournalError::Corrupt("file length changed outside journal"));
        }
        self.file
            .write_all(&length.to_be_bytes())
            .and_then(|()| self.file.write_all(&sequence.to_be_bytes()))
            .and_then(|()| self.file.write_all(&self.digest))
            .and_then(|()| self.file.write_all(payload))
            .and_then(|()| self.file.write_all(&digest))
            .and_then(|()| self.file.sync_all())
            .map_err(|error| JournalError::DurabilityUncertain(error.kind()))?;
        self.sequence = sequence;
        self.digest = digest;
        self.latest = Some(checkpoint);
        self.byte_len = next_len;
        self.poisoned = false;
        Ok(sequence)
    }

    /// Returns the most recent verified checkpoint, without deserializing it.
    #[must_use]
    pub fn latest(&self) -> Option<&[u8]> {
        self.latest.as_deref()
    }

    /// Returns the last acknowledged or recovered sequence, or zero if empty.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the account/environment identity from the validated header.
    #[must_use]
    pub const fn identity(&self) -> &JournalIdentity {
        &self.identity
    }

    fn read_frame(&mut self, file_len: u64) -> Result<(), JournalError> {
        let length = u32::from_be_bytes(read_array(&mut self.file)?);
        let payload_len = usize::try_from(length).map_err(|_| JournalError::PayloadTooLarge)?;
        if payload_len > MAX_PAYLOAD_BYTES {
            return Err(JournalError::PayloadTooLarge);
        }
        let next_len = self
            .byte_len
            .checked_add(FRAME_OVERHEAD_BYTES + u64::from(length))
            .filter(|size| *size <= file_len)
            .ok_or(JournalError::Corrupt("incomplete frame"))?;
        let sequence = u64::from_be_bytes(read_array(&mut self.file)?);
        let expected_sequence = self
            .sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        if sequence != expected_sequence {
            return Err(JournalError::Corrupt("nonconsecutive frame sequence"));
        }
        let previous: [u8; 32] = read_array(&mut self.file)?;
        if previous != self.digest {
            return Err(JournalError::Corrupt("previous frame digest mismatch"));
        }
        let mut payload = vec![0; payload_len];
        read_exact(&mut self.file, &mut payload)?;
        let digest: [u8; 32] = read_array(&mut self.file)?;
        if digest != frame_digest(length, sequence, &previous, &payload) {
            return Err(JournalError::Corrupt("frame checksum mismatch"));
        }
        self.latest = Some(payload);
        self.digest = digest;
        self.sequence = sequence;
        self.byte_len = next_len;
        Ok(())
    }
}

/// Sanitized journal failures, with no paths, credentials, or checkpoint bytes.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JournalError {
    /// Account label failed validation.
    #[error("invalid journal account identity")]
    InvalidIdentity,
    /// A parent is not a directory or the target is not a regular file.
    #[error("journal path must name a regular file in a trusted directory")]
    UnsafePath,
    /// Another cooperating process already holds the exclusive file lock.
    #[error("journal is already locked")]
    Locked,
    /// A filesystem operation failed; the source path/message is not retained.
    #[error("journal filesystem operation {operation} failed: {kind:?}")]
    Io {
        /// Non-sensitive operation name.
        operation: &'static str,
        /// Portable error category, without its potentially sensitive message.
        kind: io::ErrorKind,
    },
    /// Header serialization failed.
    #[error("journal identity serialization failed")]
    Serialization,
    /// The file was written using an unsupported format version.
    #[error("unsupported journal format version")]
    UnsupportedVersion,
    /// The journal belongs to a different exchange, account, or environment.
    #[error("journal account identity does not match")]
    IdentityMismatch,
    /// A structural or integrity check failed; no automatic repair is attempted.
    #[error("journal integrity validation failed: {0}")]
    Corrupt(&'static str),
    /// A checkpoint exceeds the maximum bounded payload size.
    #[error("journal checkpoint exceeds payload limit")]
    PayloadTooLarge,
    /// The journal requires explicit operator-managed archival before reuse.
    #[error("journal file capacity exceeded")]
    CapacityExceeded,
    /// The monotonic sequence number cannot be incremented.
    #[error("journal sequence exhausted")]
    SequenceExhausted,
    /// A write or sync may have committed despite the operation returning error.
    #[error("journal durability uncertain after filesystem failure: {0:?}")]
    DurabilityUncertain(io::ErrorKind),
    /// A previous uncertain write makes further appends unsafe.
    #[error("journal handle is poisoned; reopen and reconcile before continuing")]
    Poisoned,
}

fn io_error(operation: &'static str, error: &io::Error) -> JournalError {
    JournalError::Io {
        operation,
        kind: error.kind(),
    }
}

fn validate_parent(path: &Path) -> Result<PathBuf, JournalError> {
    if path.file_name().is_none() {
        return Err(JournalError::UnsafePath);
    }
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let metadata =
        fs::symlink_metadata(parent).map_err(|error| io_error("inspect parent", &error))?;
    if !metadata.file_type().is_dir() {
        return Err(JournalError::UnsafePath);
    }
    Ok(parent.to_path_buf())
}

fn check_regular_file(file: &File) -> Result<u64, JournalError> {
    let metadata = file
        .metadata()
        .map_err(|error| io_error("inspect open file", &error))?;
    if !metadata.is_file() {
        return Err(JournalError::UnsafePath);
    }
    Ok(metadata.len())
}

fn lock_file(file: &File) -> Result<(), JournalError> {
    file.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => JournalError::Locked,
        TryLockError::Error(error) => io_error("lock", &error),
    })
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<(), JournalError> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync parent directory", &error))
}

#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)]
fn sync_parent(_parent: &Path) -> Result<(), JournalError> {
    Ok(())
}

fn read_header(file: &mut File) -> Result<(JournalIdentity, [u8; 32], u64), JournalError> {
    let magic: [u8; 8] = read_array(file)?;
    if &magic != MAGIC {
        return Err(JournalError::Corrupt("invalid header magic"));
    }
    let version = u32::from_be_bytes(read_array(file)?);
    if version != VERSION {
        return Err(JournalError::UnsupportedVersion);
    }
    let length = u32::from_be_bytes(read_array(file)?);
    let identity_len = usize::try_from(length).map_err(|_| JournalError::InvalidIdentity)?;
    if identity_len == 0 || identity_len > MAX_IDENTITY_BYTES {
        return Err(JournalError::Corrupt("invalid identity length"));
    }
    let mut encoded = vec![0; identity_len];
    read_exact(file, &mut encoded)?;
    let digest: [u8; 32] = read_array(file)?;
    let mut hasher = Sha256::new();
    hasher.update(magic);
    hasher.update(version.to_be_bytes());
    hasher.update(length.to_be_bytes());
    hasher.update(&encoded);
    let calculated: [u8; 32] = hasher.finalize().into();
    if digest != calculated {
        return Err(JournalError::Corrupt("header checksum mismatch"));
    }
    let identity = serde_json::from_slice(&encoded)
        .map_err(|_| JournalError::Corrupt("invalid identity encoding"))?;
    Ok((
        identity,
        digest,
        HEADER_PREFIX_BYTES + u64::from(length) + DIGEST_BYTES,
    ))
}

fn frame_digest(length: u32, sequence: u64, previous: &[u8; 32], payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(length.to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    hasher.update(previous);
    hasher.update(payload);
    hasher.finalize().into()
}

fn read_exact(file: &mut File, destination: &mut [u8]) -> Result<(), JournalError> {
    file.read_exact(destination).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            JournalError::Corrupt("unexpected end of journal")
        } else {
            io_error("read", &error)
        }
    })
}

fn read_array<const LENGTH: usize>(file: &mut File) -> Result<[u8; LENGTH], JournalError> {
    let mut value = [0; LENGTH];
    read_exact(file, &mut value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use uuid::Uuid;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create() -> io::Result<Self> {
            let path = std::env::temp_dir().join(format!("ict-journal-test-{}", Uuid::new_v4()));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> PathBuf {
            self.0.join("account.journal")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            // This exact UUID directory was created by this fixture only.
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn identity() -> Result<JournalIdentity, JournalError> {
        JournalIdentity::new(Exchange::Binance, "test-account".to_owned(), true)
    }

    fn edit_bytes(path: &Path, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        file.sync_all()
    }

    #[test]
    fn identity_validates_deserialization_and_exposes_scope() -> Result<(), Box<dyn Error>> {
        let scope = identity()?;
        assert_eq!(scope.exchange(), Exchange::Binance);
        assert_eq!(scope.account_label(), "test-account");
        assert!(scope.demo());
        assert_eq!(
            serde_json::from_slice::<JournalIdentity>(&serde_json::to_vec(&scope)?)?,
            scope
        );
        for invalid in ["", "with whitespace", "secret\n", "a/b", "é"] {
            assert!(JournalIdentity::new(Exchange::Okx, invalid.to_owned(), false).is_err());
        }
        assert!(serde_json::from_str::<JournalIdentity>(
            r#"{"exchange":"binance","account_label":"bad\nlabel","demo":true}"#
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn round_trip_retains_latest_sequence_and_empty_payload() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        let mut journal = Journal::create(directory.path(), scope.clone())?;
        assert_eq!(journal.sequence(), 0);
        assert_eq!(journal.latest(), None);
        assert_eq!(journal.identity(), &scope);
        assert_eq!(journal.append(b"first checkpoint")?, 1);
        assert_eq!(journal.append(b"second checkpoint")?, 2);
        drop(journal);
        let mut reopened = Journal::open(directory.path(), &scope)?;
        assert_eq!(reopened.sequence(), 2);
        assert_eq!(reopened.latest(), Some(b"second checkpoint".as_slice()));
        assert_eq!(reopened.append(b"")?, 3);
        drop(reopened);
        let final_state = Journal::open(directory.path(), &scope)?;
        assert_eq!(final_state.latest(), Some(b"".as_slice()));
        assert_eq!(final_state.sequence(), 3);
        Ok(())
    }

    #[test]
    fn create_never_overwrites_and_open_never_creates() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        assert!(matches!(
            Journal::open(directory.path(), &scope),
            Err(JournalError::Io {
                kind: io::ErrorKind::NotFound,
                ..
            })
        ));
        assert!(!directory.path().exists());
        let journal = Journal::create(directory.path(), scope.clone())?;
        let original_len = journal.byte_len;
        drop(journal);
        assert!(matches!(
            Journal::create(directory.path(), scope),
            Err(JournalError::Io {
                kind: io::ErrorKind::AlreadyExists,
                ..
            })
        ));
        assert_eq!(fs::metadata(directory.path())?.len(), original_len);
        Ok(())
    }

    #[test]
    fn exclusive_lock_is_held_until_drop() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        let journal = Journal::create(directory.path(), scope.clone())?;
        assert!(matches!(
            Journal::open(directory.path(), &scope),
            Err(JournalError::Locked)
        ));
        drop(journal);
        assert!(Journal::open(directory.path(), &scope).is_ok());
        Ok(())
    }

    #[test]
    fn rejects_each_identity_dimension() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        drop(Journal::create(directory.path(), identity()?)?);
        let wrong_scopes = [
            JournalIdentity::new(Exchange::Bybit, "test-account".to_owned(), true)?,
            JournalIdentity::new(Exchange::Binance, "other-account".to_owned(), true)?,
            JournalIdentity::new(Exchange::Binance, "test-account".to_owned(), false)?,
        ];
        for scope in wrong_scopes {
            assert!(matches!(
                Journal::open(directory.path(), &scope),
                Err(JournalError::IdentityMismatch)
            ));
        }
        Ok(())
    }

    #[test]
    fn rejects_unsupported_version_without_repair() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        drop(Journal::create(directory.path(), scope.clone())?);
        edit_bytes(&directory.path(), 8, &2_u32.to_be_bytes())?;
        assert!(matches!(
            Journal::open(directory.path(), &scope),
            Err(JournalError::UnsupportedVersion)
        ));
        Ok(())
    }

    #[test]
    fn corrupt_header_and_last_payload_fail_closed() -> Result<(), Box<dyn Error>> {
        for offset_in_header in [true, false] {
            let directory = TestDirectory::create()?;
            let scope = identity()?;
            let mut journal = Journal::create(directory.path(), scope.clone())?;
            let header_len = journal.byte_len;
            journal.append(b"secret-checkpoint")?;
            let length = journal.byte_len;
            drop(journal);
            let offset = if offset_in_header {
                16
            } else {
                header_len + 44
            };
            edit_bytes(&directory.path(), offset, b"!")?;
            let result = Journal::open(directory.path(), &scope);
            assert!(matches!(result, Err(JournalError::Corrupt(_))));
            assert_eq!(fs::metadata(directory.path())?.len(), length);
            assert!(!format!("{result:?}").contains("secret-checkpoint"));
        }
        Ok(())
    }

    #[test]
    fn rejects_truncation_at_every_incomplete_frame_boundary() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        let mut journal = Journal::create(directory.path(), scope.clone())?;
        journal.append(b"first")?;
        let valid_prefix_len = journal.byte_len;
        journal.append(b"second")?;
        drop(journal);
        let intact = fs::read(directory.path())?;
        for length in valid_prefix_len + 1..u64::try_from(intact.len())? {
            fs::write(directory.path(), &intact)?;
            OpenOptions::new()
                .write(true)
                .open(directory.path())?
                .set_len(length)?;
            assert!(Journal::open(directory.path(), &scope).is_err());
            assert_eq!(fs::metadata(directory.path())?.len(), length);
        }
        Ok(())
    }

    #[test]
    fn rejects_sequence_gap_and_previous_digest_corruption() -> Result<(), Box<dyn Error>> {
        for mutate_sequence in [true, false] {
            let directory = TestDirectory::create()?;
            let scope = identity()?;
            let mut journal = Journal::create(directory.path(), scope.clone())?;
            let start = journal.byte_len;
            journal.append(b"payload")?;
            drop(journal);
            if mutate_sequence {
                edit_bytes(&directory.path(), start + 4, &2_u64.to_be_bytes())?;
            } else {
                edit_bytes(&directory.path(), start + 12, &[0; 32])?;
            }
            assert!(matches!(
                Journal::open(directory.path(), &scope),
                Err(JournalError::Corrupt(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn payload_capacity_and_sequence_limits_do_not_write() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let mut journal = Journal::create(directory.path(), identity()?)?;
        let original_len = journal.byte_len;
        assert_eq!(
            journal.append(&vec![0; MAX_PAYLOAD_BYTES + 1]),
            Err(JournalError::PayloadTooLarge)
        );
        journal.byte_len = MAX_FILE_BYTES;
        assert_eq!(journal.append(b""), Err(JournalError::CapacityExceeded));
        journal.byte_len = original_len;
        journal.sequence = u64::MAX;
        assert_eq!(journal.append(b""), Err(JournalError::SequenceExhausted));
        assert!(!journal.poisoned);
        assert_eq!(fs::metadata(directory.path())?.len(), original_len);
        Ok(())
    }

    #[test]
    fn oversized_open_and_forged_frame_length_are_bounded() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        let mut journal = Journal::create(directory.path(), scope.clone())?;
        let frame_start = journal.byte_len;
        journal.append(b"one")?;
        drop(journal);
        edit_bytes(&directory.path(), frame_start, &u32::MAX.to_be_bytes())?;
        assert!(matches!(
            Journal::open(directory.path(), &scope),
            Err(JournalError::PayloadTooLarge)
        ));
        OpenOptions::new()
            .write(true)
            .open(directory.path())?
            .set_len(MAX_FILE_BYTES + 1)?;
        assert!(matches!(
            Journal::open(directory.path(), &scope),
            Err(JournalError::CapacityExceeded)
        ));
        Ok(())
    }

    #[test]
    fn external_length_change_poisons_writer() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let mut journal = Journal::create(directory.path(), identity()?)?;
        // The test owns the file handle and intentionally violates its invariant.
        journal.file.set_len(journal.byte_len + 1)?;
        assert!(matches!(
            journal.append(b"unsafe"),
            Err(JournalError::Corrupt(_))
        ));
        assert_eq!(journal.append(b"retry"), Err(JournalError::Poisoned));
        assert_eq!(journal.sequence(), 0);
        Ok(())
    }

    #[test]
    fn write_failure_poisons_handle_without_changing_acknowledged_state(
    ) -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let mut journal = Journal::create(directory.path(), identity()?)?;
        journal.append(b"acknowledged")?;
        // Replace the private handle with read-only access to inject a real I/O
        // failure deterministically, without modifying process permissions.
        journal.file = File::open(directory.path())?;
        assert!(matches!(
            journal.append(b"not-written"),
            Err(JournalError::DurabilityUncertain(_))
        ));
        assert_eq!(journal.sequence(), 1);
        assert_eq!(journal.latest(), Some(b"acknowledged".as_slice()));
        assert_eq!(journal.append(b"retry"), Err(JournalError::Poisoned));
        drop(journal);
        let reopened = Journal::open(directory.path(), &identity()?)?;
        assert_eq!(reopened.sequence(), 1);
        Ok(())
    }

    #[test]
    fn debug_never_contains_checkpoint_bytes_or_path() -> Result<(), Box<dyn Error>> {
        let directory = TestDirectory::create()?;
        let mut journal = Journal::create(directory.path(), identity()?)?;
        journal.append(b"private-checkpoint-data")?;
        let output = format!("{journal:?}");
        assert!(!output.contains("private-checkpoint-data"));
        assert!(!output.contains("account.journal"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_leaf_and_parent() -> Result<(), Box<dyn Error>> {
        use std::os::unix::fs::symlink;
        let directory = TestDirectory::create()?;
        let scope = identity()?;
        drop(Journal::create(directory.path(), scope.clone())?);
        let link = directory.0.join("link.journal");
        symlink(directory.path(), &link)?;
        assert!(matches!(
            Journal::open(&link, &scope),
            Err(JournalError::UnsafePath)
        ));
        let parent_link = directory.0.join("linked-parent");
        symlink(&directory.0, &parent_link)?;
        assert!(matches!(
            Journal::create(parent_link.join("new.journal"), scope),
            Err(JournalError::UnsafePath)
        ));
        Ok(())
    }
}
