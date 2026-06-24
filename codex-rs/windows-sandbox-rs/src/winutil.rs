use anyhow::Result;
use anyhow::anyhow;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use windows_sys::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER;
use windows_sys::Win32::Foundation::GetLastError;
use windows_sys::Win32::Foundation::HLOCAL;
use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
use windows_sys::Win32::Security::CopySid;
use windows_sys::Win32::Security::GetLengthSid;
use windows_sys::Win32::Security::LookupAccountNameW;
use windows_sys::Win32::Security::SID_NAME_USE;
use windows_sys::Win32::Storage::FileSystem::GetBinaryTypeW;
use windows_sys::Win32::System::Diagnostics::Debug::FORMAT_MESSAGE_ALLOCATE_BUFFER;
use windows_sys::Win32::System::Diagnostics::Debug::FORMAT_MESSAGE_FROM_SYSTEM;
use windows_sys::Win32::System::Diagnostics::Debug::FORMAT_MESSAGE_IGNORE_INSERTS;
use windows_sys::Win32::System::Diagnostics::Debug::FormatMessageW;

pub fn to_wide<S: AsRef<OsStr>>(s: S) -> Vec<u16> {
    let mut v: Vec<u16> = s.as_ref().encode_wide().collect();
    v.push(0);
    v
}

/// Quote a single Windows command-line argument following the rules used by
/// CommandLineToArgvW/CRT so that spaces, quotes, and backslashes are preserved.
/// Reference behavior matches Rust std::process::Command on Windows.
#[cfg(target_os = "windows")]
pub fn quote_windows_arg(arg: &str) -> String {
    let needs_quotes = arg.is_empty()
        || arg
            .chars()
            .any(|c| matches!(c, ' ' | '\t' | '\n' | '\r' | '"'));
    if !needs_quotes {
        return arg.to_string();
    }

    let mut quoted = String::with_capacity(arg.len() + 2);
    quoted.push('"');
    let mut backslashes = 0;
    for ch in arg.chars() {
        match ch {
            '\\' => {
                backslashes += 1;
            }
            '"' => {
                quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                if backslashes > 0 {
                    quoted.push_str(&"\\".repeat(backslashes));
                    backslashes = 0;
                }
                quoted.push(ch);
            }
        }
    }
    if backslashes > 0 {
        quoted.push_str(&"\\".repeat(backslashes * 2));
    }
    quoted.push('"');
    quoted
}

/// Build a Windows command line for CreateProcess-style APIs.
#[cfg(target_os = "windows")]
pub fn argv_to_command_line(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| quote_windows_arg(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Resolves a Windows program using the environment and working directory that
/// will be passed to the child process.
pub fn resolve_windows_executable(
    program: &str,
    cwd: &Path,
    env_map: &HashMap<String, String>,
) -> Result<PathBuf> {
    if program.is_empty() {
        return Err(anyhow!("cannot resolve an empty Windows executable"));
    }
    if !cwd.is_absolute() {
        return Err(anyhow!("Windows executable cwd must be absolute"));
    }

    let program_path = Path::new(program);
    if is_drive_relative(program_path) {
        return Err(anyhow!(
            "drive-relative Windows executable paths are not supported"
        ));
    }
    let has_path = program_path.is_absolute()
        || program.contains(['\\', '/'])
        || program_path.components().count() > 1;
    let search_dirs = if has_path {
        Vec::new()
    } else {
        std::iter::once(cwd.to_path_buf())
            .chain(
                windows_env_value(env_map, "PATH")
                    .into_iter()
                    .flat_map(|path| std::env::split_paths(path)),
            )
            .filter(|dir| !is_drive_relative(dir))
            .map(|dir| {
                if dir.is_absolute() {
                    dir
                } else {
                    cwd.join(dir)
                }
            })
            .collect()
    };
    let path_extensions = windows_env_value(env_map, "PATHEXT")
        .unwrap_or(".EXE")
        .split(';')
        .map(str::trim)
        .filter(|extension| extension.starts_with('.') && extension.len() > 1)
        .collect::<Vec<_>>();
    let has_extension = program_path.extension().is_some();

    let candidates = if has_path {
        vec![if program_path.is_absolute() {
            program_path.to_path_buf()
        } else {
            cwd.join(program_path)
        }]
    } else {
        search_dirs
            .into_iter()
            .map(|dir| dir.join(program_path))
            .collect()
    };
    for candidate in candidates {
        if !candidate.is_absolute() {
            continue;
        }
        if (has_path || has_extension) && is_windows_executable_candidate(&candidate) {
            return Ok(candidate);
        }
        if !has_extension {
            for extension in &path_extensions {
                let mut candidate = candidate.clone().into_os_string();
                candidate.push(extension);
                let candidate = PathBuf::from(candidate);
                if is_windows_executable_candidate(&candidate) {
                    return Ok(candidate);
                }
            }
        }
    }

    Err(anyhow!(
        "Windows executable `{program}` was not found using the child PATH and PATHEXT"
    ))
}

fn is_drive_relative(path: &Path) -> bool {
    !path.has_root()
        && matches!(
            path.components().next(),
            Some(std::path::Component::Prefix(prefix))
                if matches!(prefix.kind(), std::path::Prefix::Disk(_))
        )
}

fn windows_env_value<'a>(env_map: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    env_map
        .get(key)
        .or_else(|| {
            env_map
                .iter()
                .find(|(existing, _)| existing.eq_ignore_ascii_case(key))
                .map(|(_, value)| value)
        })
        .map(String::as_str)
}

fn is_windows_executable_candidate(path: &Path) -> bool {
    let validation_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let path = to_wide(validation_path);
    let mut binary_type = 0;
    unsafe { GetBinaryTypeW(path.as_ptr(), &mut binary_type) != 0 }
}

// Produce a readable description for a Win32 error code.
pub fn format_last_error(err: i32) -> String {
    unsafe {
        let mut buf_ptr: *mut u16 = std::ptr::null_mut();
        let flags = FORMAT_MESSAGE_ALLOCATE_BUFFER
            | FORMAT_MESSAGE_FROM_SYSTEM
            | FORMAT_MESSAGE_IGNORE_INSERTS;
        let len = FormatMessageW(
            flags,
            std::ptr::null(),
            err as u32,
            0,
            // FORMAT_MESSAGE_ALLOCATE_BUFFER expects a pointer to receive the allocated buffer.
            // Cast &mut *mut u16 to *mut u16 as required by windows-sys.
            (&mut buf_ptr as *mut *mut u16) as *mut u16,
            0,
            std::ptr::null_mut(),
        );
        if len == 0 || buf_ptr.is_null() {
            return format!("Win32 error {err}");
        }
        let slice = std::slice::from_raw_parts(buf_ptr, len as usize);
        let mut s = String::from_utf16_lossy(slice);
        s = s.trim().to_string();
        let _ = LocalFree(buf_ptr as HLOCAL);
        s
    }
}

pub fn string_from_sid_bytes(sid: &[u8]) -> Result<String, String> {
    unsafe {
        let mut str_ptr: *mut u16 = std::ptr::null_mut();
        let ok = ConvertSidToStringSidW(sid.as_ptr() as *mut std::ffi::c_void, &mut str_ptr);
        if ok == 0 || str_ptr.is_null() {
            return Err(format!(
                "ConvertSidToStringSidW failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let mut len = 0;
        while *str_ptr.add(len) != 0 {
            len += 1;
        }
        let slice = std::slice::from_raw_parts(str_ptr, len);
        let out = String::from_utf16_lossy(slice);
        let _ = LocalFree(str_ptr as HLOCAL);
        Ok(out)
    }
}

const SID_ADMINISTRATORS: &str = "S-1-5-32-544";
const SID_USERS: &str = "S-1-5-32-545";
const SID_AUTHENTICATED_USERS: &str = "S-1-5-11";
const SID_EVERYONE: &str = "S-1-1-0";
const SID_SYSTEM: &str = "S-1-5-18";

pub fn resolve_sid(name: &str) -> Result<Vec<u8>> {
    if let Some(sid_str) = well_known_sid_str(name) {
        return sid_bytes_from_string(sid_str);
    }
    let name_w = to_wide(OsStr::new(name));
    let mut sid_buffer = vec![0u8; 68];
    let mut sid_len: u32 = sid_buffer.len() as u32;
    let mut domain: Vec<u16> = Vec::new();
    let mut domain_len: u32 = 0;
    let mut use_type: SID_NAME_USE = 0;
    loop {
        let ok = unsafe {
            LookupAccountNameW(
                std::ptr::null(),
                name_w.as_ptr(),
                sid_buffer.as_mut_ptr() as *mut std::ffi::c_void,
                &mut sid_len,
                domain.as_mut_ptr(),
                &mut domain_len,
                &mut use_type,
            )
        };
        if ok != 0 {
            sid_buffer.truncate(sid_len as usize);
            return Ok(sid_buffer);
        }
        let err = unsafe { GetLastError() };
        if err == ERROR_INSUFFICIENT_BUFFER {
            sid_buffer.resize(sid_len as usize, 0);
            domain.resize(domain_len as usize, 0);
            continue;
        }
        return Err(anyhow::anyhow!(
            "LookupAccountNameW failed for {name}: {err}"
        ));
    }
}

fn well_known_sid_str(name: &str) -> Option<&'static str> {
    match name {
        "Administrators" => Some(SID_ADMINISTRATORS),
        "Users" => Some(SID_USERS),
        "Authenticated Users" => Some(SID_AUTHENTICATED_USERS),
        "Everyone" => Some(SID_EVERYONE),
        "SYSTEM" => Some(SID_SYSTEM),
        _ => None,
    }
}

fn sid_bytes_from_string(sid_str: &str) -> Result<Vec<u8>> {
    let sid_w = to_wide(OsStr::new(sid_str));
    let mut psid: *mut std::ffi::c_void = std::ptr::null_mut();
    if unsafe { ConvertStringSidToSidW(sid_w.as_ptr(), &mut psid) } == 0 {
        return Err(anyhow::anyhow!(
            "ConvertStringSidToSidW failed for {sid_str}: {}",
            unsafe { GetLastError() }
        ));
    }
    let sid_len = unsafe { GetLengthSid(psid) };
    if sid_len == 0 {
        unsafe {
            LocalFree(psid as _);
        }
        return Err(anyhow::anyhow!("GetLengthSid failed for {sid_str}"));
    }
    let mut out = vec![0u8; sid_len as usize];
    let ok = unsafe { CopySid(sid_len, out.as_mut_ptr() as *mut std::ffi::c_void, psid) };
    unsafe {
        LocalFree(psid as _);
    }
    if ok == 0 {
        return Err(anyhow::anyhow!("CopySid failed for {sid_str}"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::argv_to_command_line;
    use super::resolve_windows_executable;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::fs;
    use std::os::windows::ffi::OsStrExt;
    use tempfile::TempDir;

    fn copy_test_executable(path: &std::path::Path) {
        fs::copy(std::env::current_exe().expect("test executable"), path)
            .expect("copy executable fixture");
    }

    #[test]
    fn argv_to_command_line_quotes_each_argument_independently() {
        let argv = vec![
            "cmd.exe".to_string(),
            "/c".to_string(),
            "\"C:\\Program Files\\PowerShell\\7\\pwsh.exe\" -NoProfile -EncodedCommand abc=="
                .to_string(),
        ];

        assert_eq!(
            argv_to_command_line(&argv),
            "cmd.exe /c \"\\\"C:\\Program Files\\PowerShell\\7\\pwsh.exe\\\" -NoProfile -EncodedCommand abc==\""
        );
    }

    #[test]
    fn argv_to_command_line_quotes_regular_program_args() {
        let argv = vec![
            "pwsh.exe".to_string(),
            "-Command".to_string(),
            "Write-Output \"hello world\"".to_string(),
        ];

        assert_eq!(
            argv_to_command_line(&argv),
            "pwsh.exe -Command \"Write-Output \\\"hello world\\\"\""
        );
    }

    #[test]
    fn bare_names_search_child_cwd_before_path_and_ignore_extensionless_files() {
        let tempdir = TempDir::new().expect("tempdir");
        fs::write(tempdir.path().join("tool"), []).expect("write decoy fixture");
        let expected = tempdir.path().join("tool.exe");
        copy_test_executable(&expected);
        let bin = tempdir.path().join("bin");
        fs::create_dir(&bin).expect("create PATH directory");
        copy_test_executable(&bin.join("tool.exe"));
        let env_map = HashMap::from([("PATH".to_string(), bin.display().to_string())]);

        let resolved = resolve_windows_executable("tool", tempdir.path(), &env_map)
            .expect("resolve executable suffix");

        assert_eq!(resolved, expected);
    }

    #[test]
    fn preserves_long_child_working_directories() {
        let tempdir = TempDir::new().expect("tempdir");
        let mut cwd = tempdir.path().join("workspace");
        while cwd.as_os_str().encode_wide().count() <= 280 {
            cwd.push("long-working-directory-segment");
        }
        let bin = cwd.join("bin");
        fs::create_dir_all(&bin).expect("create long working directory");
        let expected = bin.join("tool.exe");
        copy_test_executable(&expected);
        let env_map = HashMap::from([("PATH".to_string(), "bin".to_string())]);

        let resolved = resolve_windows_executable("tool.exe", &cwd, &env_map)
            .expect("resolve from long child working directory");

        assert_eq!(resolved, expected);
    }

    #[test]
    fn rejects_ambient_path_fallback_and_ambiguous_names() {
        let tempdir = TempDir::new().expect("tempdir");
        fs::write(tempdir.path().join("tool.txt.exe"), []).expect("write decoy fixture");
        let env_map = HashMap::from([
            ("PATH".to_string(), tempdir.path().display().to_string()),
            ("PATHEXT".to_string(), ".EXE".to_string()),
        ]);

        resolve_windows_executable("cmd", tempdir.path(), &env_map)
            .expect_err("parent PATH must not be searched");
        resolve_windows_executable("tool.txt", tempdir.path(), &env_map)
            .expect_err("existing extensions must not be extended");
        resolve_windows_executable("C:tool.exe", tempdir.path(), &env_map)
            .expect_err("drive-relative paths must be rejected");
    }
}
