use std::ffi::OsStr;
use std::fmt;
use std::os::unix::prelude::OsStrExt;

#[derive(Clone, Copy)]
enum DisplayBytes<'a> {
    Str(&'a str),
    OsStr(&'a OsStr),
}

impl fmt::Display for DisplayBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DisplayBytes::Str(s) => <str as fmt::Display>::fmt(s, f),
            DisplayBytes::OsStr(s) => <OsStr as fmt::Debug>::fmt(s, f),
        }
    }
}

#[inline]
#[must_use]
pub fn display_bytes(bytes: &[u8]) -> impl fmt::Display + Copy + '_ {
    match simdutf8::basic::from_utf8(bytes) {
        Ok(s) => DisplayBytes::Str(s),
        Err(_) => DisplayBytes::OsStr(OsStr::from_bytes(bytes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        {
            let input = "world";
            let ans = display_bytes(input.as_bytes()).to_string();
            assert_eq!(ans, input);
        }
        {
            let input: &[u8] = &[0xfa, 0xfb, 0xfc, 0xfd];
            let ans = display_bytes(input).to_string();
            assert_eq!(ans, r#""\xFA\xFB\xFC\xFD""#);
        }
    }
}
