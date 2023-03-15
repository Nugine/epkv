use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BytesStr(Bytes);

impl BytesStr {
    #[inline]
    #[must_use]
    pub fn copy_from_str(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_ref()))
    }

    #[inline]
    #[allow(unsafe_code)]
    pub fn as_str(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Debug for BytesStr {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <&str as fmt::Debug>::fmt(&self.as_str(), f)
    }
}

impl From<String> for BytesStr {
    #[inline]
    fn from(s: String) -> Self {
        Self(Bytes::from(s))
    }
}

impl From<BytesStr> for Bytes {
    #[inline]
    fn from(s: BytesStr) -> Self {
        s.0
    }
}

impl AsRef<[u8]> for BytesStr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<str> for BytesStr {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Serialize for BytesStr {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <Bytes as Serialize>::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BytesStr {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <Bytes as Deserialize<'de>>::deserialize(deserializer)?;
        match simdutf8::basic::from_utf8(&bytes) {
            Ok(_) => Ok(Self(bytes)),
            Err(err) => Err(serde::de::Error::custom(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec;

    #[test]
    fn simple() {
        let s = BytesStr::copy_from_str("hello");
        assert_eq!(s.as_str(), "hello");
    }

    #[test]
    fn serde() {
        {
            let s1 = BytesStr::copy_from_str("hello");
            let bytes = codec::serialize(&s1).unwrap();
            let s2 = codec::deserialize_owned(&bytes).unwrap();
            assert_eq!(s1, s2);
        }
        {
            let bytes: &[u8] = &[0x05, 0xff, 0xff, 0xff, 0xff, 0xff];
            assert!(codec::deserialize_owned::<BytesStr>(bytes).is_err());
        }
    }
}
