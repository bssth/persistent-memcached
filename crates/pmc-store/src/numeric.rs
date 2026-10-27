//! Number parsing with the exact semantics memcached uses (`safe_strtoull`).

fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// Parses an unsigned 64-bit number the way memcached's `safe_strtoull` does:
/// leading whitespace and a sign are allowed (`strtoull`), the number must be followed by
/// the end of input or whitespace, overflow is rejected, and a negative number is rejected
/// only when the wrapped result looks negative as a signed value.
pub fn safe_strtoull(s: &[u8]) -> Option<u64> {
    let mut i = 0;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let digits_start = i;
    let mut v: u64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        v = v.checked_mul(10)?.checked_add((s[i] - b'0') as u64)?;
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    if i < s.len() && !is_c_space(s[i]) {
        return None;
    }
    if neg {
        v = v.wrapping_neg();
        if (v as i64) < 0 {
            return None;
        }
    }
    Some(v)
}

/// Strict parser for protocol tokens (flags, lengths, deltas): plain decimal digits only.
pub fn parse_u64_token(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(v)
}

/// Signed decimal token, e.g. exptime.
pub fn parse_i64_token(s: &[u8]) -> Option<i64> {
    match s.first() {
        Some(b'-') => {
            let v = parse_u64_token(&s[1..])?;
            if v > i64::MAX as u64 + 1 {
                return None;
            }
            Some((v as i128).wrapping_neg() as i64)
        }
        _ => {
            let v = parse_u64_token(s)?;
            i64::try_from(v).ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strtoull_semantics() {
        assert_eq!(safe_strtoull(b"10"), Some(10));
        assert_eq!(safe_strtoull(b"  7"), Some(7));
        assert_eq!(safe_strtoull(b"+7"), Some(7));
        assert_eq!(safe_strtoull(b"9 "), Some(9));
        assert_eq!(safe_strtoull(b"12 34"), Some(12));
        assert_eq!(safe_strtoull(b"-0"), Some(0));
        assert_eq!(safe_strtoull(b"-5"), None);
        assert_eq!(safe_strtoull(b"12abc"), None);
        assert_eq!(safe_strtoull(b""), None);
        assert_eq!(safe_strtoull(b"abc"), None);
        assert_eq!(safe_strtoull(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(safe_strtoull(b"18446744073709551616"), None);
    }

    #[test]
    fn tokens() {
        assert_eq!(parse_u64_token(b"0"), Some(0));
        assert_eq!(parse_u64_token(b"-1"), None);
        assert_eq!(parse_u64_token(b""), None);
        assert_eq!(parse_i64_token(b"-1"), Some(-1));
        assert_eq!(parse_i64_token(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64_token(b"9223372036854775808"), None);
    }
}
