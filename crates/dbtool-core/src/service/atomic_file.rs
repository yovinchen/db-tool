use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const TEMP_FILE_ATTEMPTS: usize = 128;
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Durably publish `content` through a same-directory temporary file.
///
/// The temporary file is written and synced before it replaces `path`. Unix
/// then syncs the parent directory; Windows uses `MoveFileExW` with both
/// replace-existing and write-through flags. Failures before replacement close
/// and remove the temporary file while leaving an existing target untouched.
pub fn write_file_atomically(path: &Path, content: &[u8]) -> io::Result<()> {
    write_file_atomically_with(path, content, |_, _| Ok(()))
}

pub(crate) fn write_file_atomically_with<F>(
    path: &Path,
    content: &[u8],
    before_replace: F,
) -> io::Result<()>
where
    F: FnOnce(&Path, &Path) -> io::Result<()>,
{
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| with_context("create atomic file parent directory", error))?;

    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("atomic file target has no file name: {}", path.display()),
        )
    })?;
    let (temporary_path, temporary_file) = create_temporary_file(parent, file_name)?;
    let mut temporary = PendingTemporaryFile::new(temporary_path, temporary_file);

    let result = (|| {
        temporary.write_and_sync(content)?;
        temporary.close();
        before_replace(temporary.path(), path)
            .map_err(|error| with_context("prepare atomic file replacement", error))?;
        replace_file(temporary.path(), path)
            .map_err(|error| with_context("replace atomic file target", error))?;
        temporary.mark_published();
        sync_parent_directory(parent)
            .map_err(|error| with_context("sync atomic file parent directory", error))
    })();

    match result {
        Ok(()) => Ok(()),
        Err(error) => match temporary.cleanup() {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(io::Error::new(
                error.kind(),
                format!(
                    "{error}; additionally failed to clean atomic temporary file: {cleanup_error}"
                ),
            )),
        },
    }
}

fn create_temporary_file(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> io::Result<(PathBuf, File)> {
    for _ in 0..TEMP_FILE_ATTEMPTS {
        let sequence = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".dbtool-tmp-{}-{sequence}", std::process::id()));
        let candidate = parent.join(temporary_name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(with_context("create atomic temporary file", error));
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("unable to allocate an atomic temporary file after {TEMP_FILE_ATTEMPTS} attempts"),
    ))
}

struct PendingTemporaryFile {
    path: PathBuf,
    file: Option<File>,
    published: bool,
}

impl PendingTemporaryFile {
    fn new(path: PathBuf, file: File) -> Self {
        Self {
            path,
            file: Some(file),
            published: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn write_and_sync(&mut self, content: &[u8]) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .expect("pending temporary file remains open until replacement");
        file.write_all(content)
            .map_err(|error| with_context("write atomic temporary file", error))?;
        file.sync_all()
            .map_err(|error| with_context("sync atomic temporary file", error))
    }

    fn close(&mut self) {
        self.file.take();
    }

    fn mark_published(&mut self) {
        self.published = true;
    }

    fn cleanup(&mut self) -> io::Result<()> {
        self.close();
        if self.published {
            return Ok(());
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl Drop for PendingTemporaryFile {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[cfg(not(windows))]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    std::fs::rename(source, target)
}

#[cfg(windows)]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    #[link(name = "Kernel32")]
    extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: Both paths are live, NUL-terminated UTF-16 buffers for the
    // duration of the call. MoveFileExW does not mutate either buffer.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> io::Result<()> {
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> io::Result<()> {
    // Windows replacement is already issued with MOVEFILE_WRITE_THROUGH.
    // Opening a directory as a regular File is not portable there.
    Ok(())
}

fn with_context(action: &str, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{action}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_existing_target_and_leaves_only_the_published_file() {
        let root = unique_test_dir("replace-existing");
        let path = root.join("nested").join("artifact.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"old").unwrap();

        write_file_atomically(&path, b"new").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pre_replace_failure_keeps_target_and_cleans_same_directory_temporary_file() {
        let root = unique_test_dir("pre-replace-failure");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("artifact.json");
        std::fs::write(&path, b"old").unwrap();

        let error = write_file_atomically_with(&path, b"new", |temporary, target| {
            assert_eq!(temporary.parent(), target.parent());
            assert!(temporary.exists());
            Err(io::Error::other("injected before replace"))
        })
        .unwrap_err();

        assert!(error.to_string().contains("injected before replace"));
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn replacement_failure_removes_the_temporary_file() {
        let root = unique_test_dir("replace-failure");
        std::fs::create_dir_all(&root).unwrap();
        let target_directory = root.join("occupied");
        std::fs::create_dir(&target_directory).unwrap();

        let error = write_file_atomically(&target_directory, b"new").unwrap_err();

        assert!(error.to_string().contains("replace atomic file target"));
        assert!(target_directory.is_dir());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(label: &str) -> PathBuf {
        let sequence = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dbtool-atomic-file-{label}-{}-{sequence}",
            std::process::id()
        ))
    }
}
