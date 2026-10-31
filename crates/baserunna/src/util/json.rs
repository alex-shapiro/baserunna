//! Pre-parse validation for raw JSON bytes.

/// Rejects JSON text that contains an integer literal outside the `i64` and `u64`
/// ranges, which `serde_json` would round to `f64`.
pub fn reject_inexact_integers(text: &[u8]) -> Result<(), String> {
    let mut i = 0;
    while i < text.len() {
        match text[i] {
            b'"' => {
                i += 1;
                while i < text.len() && text[i] != b'"' {
                    i += if text[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < text.len() && matches!(text[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-') {
                    i += 1;
                }
                let lit = std::str::from_utf8(&text[start..i]).unwrap_or_default();
                let digits = lit.strip_prefix('-').unwrap_or(lit);
                if !digits.is_empty()
                    && digits.bytes().all(|c| c.is_ascii_digit())
                    && lit.parse::<i64>().is_err()
                    && lit.parse::<u64>().is_err()
                {
                    return Err(format!("integer {lit} is outside the 64-bit range; send it as a string"));
                }
            }
            _ => i += 1,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_inexact_integers_matches_i64_and_u64_bounds() {
        use crate::util::rng::Rng;
        fn digits(r: &mut Rng) -> String {
            let n = 1 + r.usize(28);
            let mut d: String = (0..n).map(|_| char::from(b'0' + r.below(10) as u8)).collect();
            if d.len() > 1 && d.starts_with('0') {
                d.replace_range(0..1, "1");
            }
            d
        }
        fn value(r: &mut Rng, depth: usize, inexact: &mut bool) -> String {
            match r.usize(if depth > 2 { 4 } else { 6 }) {
                0 => {
                    let lit = format!("{}{}", if r.chance(0.5) { "-" } else { "" }, digits(r));
                    let v: i128 = lit.parse().unwrap();
                    *inexact |= v < i64::MIN as i128 || v > u64::MAX as i128;
                    lit
                }
                1 => format!("{}.{}e{}", digits(r), digits(r), r.below(20)),
                2 => format!("\"{}\\\" {}\"", digits(r), digits(r)),
                3 => ["true", "null", "\"\\\\\"", "-0"][r.usize(4)].to_string(),
                4 => {
                    let n = r.usize(4);
                    format!("[{}]", (0..n).map(|_| value(r, depth + 1, inexact)).collect::<Vec<_>>().join(", "))
                }
                _ => {
                    let n = r.usize(4);
                    let fields: Vec<String> =
                        (0..n).map(|k| format!("\"{k}\": {}", value(r, depth + 1, inexact))).collect();
                    format!("{{{}}}", fields.join(","))
                }
            }
        }
        crate::util::prop::check(
            "reject_inexact_integers_matches_i64_and_u64_bounds",
            5000,
            |r| {
                let mut inexact = false;
                let text = value(r, 0, &mut inexact);
                (text, inexact)
            },
            |_| Vec::new(),
            |(text, inexact)| {
                serde_json::from_str::<serde_json::Value>(text).map_err(|e| format!("{text}: {e}"))?;
                match (reject_inexact_integers(text.as_bytes()).is_err(), *inexact) {
                    (a, b) if a == b => Ok(()),
                    (rejected, _) => Err(format!("{text}: rejected {rejected}")),
                }
            },
        );
    }
}
