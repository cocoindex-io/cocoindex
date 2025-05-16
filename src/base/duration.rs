use anyhow::{Result, anyhow};
use chrono::Duration;

/// Parses a duration string into a chrono::Duration.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("Empty duration string"));
    }

    // Try ISO 8601-like format
    if s.starts_with('P') {
        let s = s
            .strip_prefix('P')
            .ok_or_else(|| anyhow!("Invalid duration: {}", s))?;
        let mut total = Duration::zero();
        let mut num = String::new();
        let mut negative = false;

        for c in s.chars() {
            match c {
                '-' if num.is_empty() => {
                    negative = true;
                }
                c if c.is_ascii_digit() => {
                    num.push(c);
                }
                unit => {
                    if num.is_empty() {
                        return Err(anyhow!("Missing number in duration: {}", s));
                    }
                    let n: i64 = num
                        .parse()
                        .map_err(|_| anyhow!("Invalid number: {}", num))?;
                    let n = if negative { -n } else { n };
                    match unit.to_ascii_uppercase() {
                        'D' => total += Duration::days(n),
                        'H' => total += Duration::hours(n),
                        'M' => total += Duration::minutes(n),
                        'S' => total += Duration::seconds(n),
                        c => return Err(anyhow!("Invalid unit in duration: {}", c)),
                    }
                    num.clear();
                    negative = false;
                }
            }
        }

        if total != Duration::zero() {
            return Ok(total);
        }
        return Err(anyhow!("Invalid duration: {}", s));
    }

    // Try human-readable format
    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut total = Duration::zero();
    let mut i = 0;

    while i < parts.len() {
        let num_str = parts[i];
        let num: i64 = num_str
            .parse()
            .map_err(|_| anyhow!("Invalid number: {}", num_str))?;
        i += 1;
        let unit = parts
            .get(i)
            .ok_or_else(|| anyhow!("Missing unit in duration: {}", s))?;
        match unit.to_lowercase().as_str() {
            "day" | "days" => total += Duration::days(num),
            "hour" | "hours" => total += Duration::hours(num),
            "minute" | "minutes" => total += Duration::minutes(num),
            "second" | "seconds" => total += Duration::seconds(num),
            "millisecond" | "milliseconds" => total += Duration::milliseconds(num),
            "microsecond" | "microseconds" => total += Duration::microseconds(num),
            unit => return Err(anyhow!("Unknown unit: {}", unit)),
        }
        i += 1;
    }

    if total == Duration::zero() {
        return Err(anyhow!("Invalid duration: {}", s));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_ok(res: Result<Duration>, expected: Duration, input_str: &str) {
        match res {
            Ok(duration) => assert_eq!(duration, expected, "Input: '{}'", input_str),
            Err(e) => panic!(
                "Input: '{}', expected Ok({:?}), but got Err: {}",
                input_str, expected, e
            ),
        }
    }

    fn check_err_contains(res: Result<Duration>, expected_substring: &str, input_str: &str) {
        match res {
            Ok(d) => panic!(
                "Input: '{}', expected error containing '{}', but got Ok({:?})",
                input_str, expected_substring, d
            ),
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains(expected_substring),
                    "Input: '{}', error message '{}' does not contain expected substring '{}'",
                    input_str,
                    err_msg,
                    expected_substring
                );
            }
        }
    }

    #[test]
    fn test_empty_string() {
        check_err_contains(parse_duration(""), "Empty duration string", "\"\"");
    }

    #[test]
    fn test_whitespace_string() {
        check_err_contains(parse_duration("   "), "Empty duration string", "\"   \"");
    }

    #[test]
    fn test_iso_just_p() {
        check_err_contains(parse_duration("P"), "Invalid duration: ", "\"P\"");
    }

    #[test]
    fn test_iso_missing_number_before_unit() {
        check_err_contains(
            parse_duration("PH"),
            "Missing number in duration: H",
            "\"PH\"",
        );
    }

    #[test]
    fn test_iso_char_t_issue() {
        check_err_contains(
            parse_duration("PT1H"),
            "Missing number in duration: T1H",
            "\"PT1H\"",
        );
    }

    #[test]
    fn test_iso_invalid_number_parse() {
        check_err_contains(
            parse_duration("P99999999999999999999H"),
            "Invalid number: 99999999999999999999",
            "\"P99999999999999999999H\"",
        );
    }

    #[test]
    fn test_iso_invalid_unit() {
        check_err_contains(
            parse_duration("P1X"),
            "Invalid unit in duration: X",
            "\"P1X\"",
        );
    }

    #[test]
    fn test_iso_valid_days() {
        check_ok(parse_duration("P1D"), Duration::days(1), "\"P1D\"");
    }

    #[test]
    fn test_iso_valid_hours() {
        check_ok(parse_duration("P2H"), Duration::hours(2), "\"P2H\"");
    }

    #[test]
    fn test_iso_valid_minutes() {
        check_ok(parse_duration("P3M"), Duration::minutes(3), "\"P3M\"");
    }

    #[test]
    fn test_iso_valid_seconds() {
        check_ok(parse_duration("P4S"), Duration::seconds(4), "\"P4S\"");
    }

    #[test]
    fn test_iso_valid_lowercase_p_and_unit() {
        check_err_contains(parse_duration("p1h"), "Invalid number: p1h", "\"p1h\"");
    }

    #[test]
    fn test_iso_valid_uppercase_p_lowercase_unit() {
        check_ok(parse_duration("P1h"), Duration::hours(1), "\"P1h\"");
    }

    #[test]
    fn test_iso_combined_units() {
        check_ok(
            parse_duration("P1D2H3M4S"),
            Duration::days(1) + Duration::hours(2) + Duration::minutes(3) + Duration::seconds(4),
            "\"P1D2H3M4S\"",
        );
    }

    #[test]
    fn test_iso_trailing_number_bug_demonstration_1() {
        check_ok(
            parse_duration("P1D2"),
            Duration::days(1),
            "\"P1D2\" (bug: trailing '2' ignored)",
        );
    }

    #[test]
    fn test_iso_trailing_number_bug_demonstration_2() {
        check_err_contains(parse_duration("P1"), "Invalid duration: 1", "\"P1\"");
    }

    #[test]
    fn test_iso_zero_duration_pd0() {
        check_err_contains(parse_duration("P0D"), "Invalid duration: 0D", "\"P0D\"");
    }

    #[test]
    fn test_iso_zero_duration_pt0s_fail() {
        check_err_contains(
            parse_duration("PT0S"),
            "Missing number in duration: T0S",
            "\"PT0S\"",
        );
    }

    #[test]
    fn test_iso_fractional_seconds_fail() {
        check_err_contains(
            parse_duration("PT1.5S"),
            "Missing number in duration: T1.5S",
            "\"PT1.5S\"",
        );
    }

    #[test]
    fn test_human_invalid_number_at_start() {
        check_err_contains(
            parse_duration("one day"),
            "Invalid number: one",
            "\"one day\"",
        );
    }

    #[test]
    fn test_human_float_number_fail() {
        check_err_contains(
            parse_duration("1.5 hours"),
            "Invalid number: 1.5",
            "\"1.5 hours\"",
        );
    }

    #[test]
    fn test_human_missing_unit_after_number_at_end() {
        check_err_contains(parse_duration("1"), "Missing unit in duration: 1", "\"1\"");
    }

    #[test]
    fn test_human_missing_unit_after_number_in_middle() {
        check_err_contains(
            parse_duration("1 day 2"),
            "Missing unit in duration: 1 day 2",
            "\"1 day 2\"",
        );
    }

    #[test]
    fn test_human_unknown_unit() {
        check_err_contains(parse_duration("1 year"), "Unknown unit: year", "\"1 year\"");
    }

    #[test]
    fn test_human_valid_day() {
        check_ok(parse_duration("1 day"), Duration::days(1), "\"1 day\"");
    }

    #[test]
    fn test_human_valid_days_uppercase() {
        check_ok(parse_duration("2 DAYS"), Duration::days(2), "\"2 DAYS\"");
    }

    #[test]
    fn test_human_valid_hour() {
        check_ok(parse_duration("3 hour"), Duration::hours(3), "\"3 hour\"");
    }

    #[test]
    fn test_human_valid_hours_mixedcase() {
        check_ok(parse_duration("4 HoUrS"), Duration::hours(4), "\"4 HoUrS\"");
    }

    #[test]
    fn test_human_valid_minute() {
        check_ok(
            parse_duration("5 minute"),
            Duration::minutes(5),
            "\"5 minute\"",
        );
    }

    #[test]
    fn test_human_valid_minutes() {
        check_ok(
            parse_duration("6 minutes"),
            Duration::minutes(6),
            "\"6 minutes\"",
        );
    }

    #[test]
    fn test_human_valid_second() {
        check_ok(
            parse_duration("7 second"),
            Duration::seconds(7),
            "\"7 second\"",
        );
    }

    #[test]
    fn test_human_valid_seconds() {
        check_ok(
            parse_duration("8 seconds"),
            Duration::seconds(8),
            "\"8 seconds\"",
        );
    }

    #[test]
    fn test_human_valid_millisecond() {
        check_ok(
            parse_duration("9 millisecond"),
            Duration::milliseconds(9),
            "\"9 millisecond\"",
        );
    }

    #[test]
    fn test_human_valid_milliseconds() {
        check_ok(
            parse_duration("10 milliseconds"),
            Duration::milliseconds(10),
            "\"10 milliseconds\"",
        );
    }

    #[test]
    fn test_human_valid_microsecond() {
        check_ok(
            parse_duration("11 microsecond"),
            Duration::microseconds(11),
            "\"11 microsecond\"",
        );
    }

    #[test]
    fn test_human_valid_microseconds() {
        check_ok(
            parse_duration("12 microseconds"),
            Duration::microseconds(12),
            "\"12 microseconds\"",
        );
    }

    #[test]
    fn test_human_combined_units() {
        let expected = Duration::days(1)
            + Duration::hours(2)
            + Duration::minutes(30)
            + Duration::seconds(15)
            + Duration::milliseconds(100)
            + Duration::microseconds(200);
        check_ok(
            parse_duration("1 day 2 hours 30 minutes 15 seconds 100 milliseconds 200 microseconds"),
            expected,
            "\"1 day 2 hours 30 minutes 15 seconds 100 milliseconds 200 microseconds\"",
        );
    }

    #[test]
    fn test_human_zero_duration_seconds() {
        check_err_contains(
            parse_duration("0 seconds"),
            "Invalid duration: 0 seconds",
            "\"0 seconds\"",
        );
    }

    #[test]
    fn test_human_zero_duration_days_hours() {
        check_err_contains(
            parse_duration("0 day 0 hour"),
            "Invalid duration: 0 day 0 hour",
            "\"0 day 0 hour\"",
        );
    }

    #[test]
    fn test_human_no_space_between_num_unit() {
        check_err_contains(parse_duration("1day"), "Invalid number: 1day", "\"1day\"");
    }

    #[test]
    fn test_human_extra_whitespace() {
        check_ok(
            parse_duration("  1  day   2  hours "),
            Duration::days(1) + Duration::hours(2),
            "\"  1  day   2  hours \"",
        );
    }

    #[test]
    fn test_human_just_unit_no_number() {
        check_err_contains(parse_duration("day"), "Invalid number: day", "\"day\"");
    }
    #[test]
    fn test_human_negative_numbers() {
        check_ok(
            parse_duration("-1 day 2 hours"),
            Duration::days(-1) + Duration::hours(2),
            "\"-1 day 2 hours\"",
        );
        check_ok(
            parse_duration("1 day -2 hours"),
            Duration::days(1) + Duration::hours(-2),
            "\"1 day -2 hours\"",
        );
    }

    #[test]
    fn test_iso_negative_numbers() {
        check_ok(parse_duration("P-1D"), Duration::days(-1), "\"P-1D\"");
        check_ok(
            parse_duration("P-1H-2M"),
            Duration::hours(-1) + Duration::minutes(-2),
            "\"P-1H-2M\"",
        );
        check_err_contains(
            parse_duration("P-1-2H"),
            "Invalid unit in duration: -",
            "\"P-1-2H\"",
        );
    }
}
