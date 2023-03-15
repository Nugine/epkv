#![allow(unsafe_code)]

#[inline]
pub fn vec_to_string(v: Vec<u8>) -> Result<String, simdutf8::basic::Utf8Error> {
    simdutf8::basic::from_utf8(&v)?;
    Ok(unsafe { String::from_utf8_unchecked(v) })
}
