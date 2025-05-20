use anyhow::{Result, anyhow, bail};
use chrono::Duration;

/// Parses a string of number-unit pairs into a vector of (number, unit),
/// ensuring units are among the allowed ones.
fn parse_components(
    s: &str,
    allowed_units: &[char],
    original_input: &str,
) -> Result<Vec<(i64, char)>> {
    let mut result = Vec::new();
    let mut iter = s.chars().peekable();
    while iter.peek().is_some() {
        let mut num_str = String::new();
        while let Some(&c) = iter.peek() {
            if c.is_digit(10) {
                num_str.push(iter.next().unwrap());
            } else {
                break;
            }
        }
        if num_str.is_empty() {
            bail!("Expected number in: {}", original_input);
        }
        let num = num_str
            .parse()
            .map_err(|_| anyhow!("Invalid number '{}' in: {}", num_str, original_input))?;
        if let Some(&unit) = iter.peek() {
            if allowed_units.contains(&unit) {
                result.push((num, unit));
                iter.next();
            } else {
                bail!("Invalid unit '{}' in: {}", unit, original_input);
            }
        } else {
            bail!(
                "Missing unit after number '{}' in: {}",
                num_str,
                original_input
            );
        }
    }
    Ok(result)
}

/// Parses an ISO 8601 duration string into a `chrono::Duration`.
fn parse_iso8601_duration(s: &str, original_input: &str) -> Result<Duration> {
    let (is_negative, s_after_sign) = if s.starts_with('-') {
        (true, &s[1..])
    } else {
        (false, s)
    };

    if !s_after_sign.starts_with('P') {
        bail!("Duration must start with 'P' in: {}", original_input);
    }
    let s_after_p = &s_after_sign[1..];

    let (date_part, time_part) = if let Some(pos) = s_after_p.find('T') {
        (&s_after_p[..pos], Some(&s_after_p[pos + 1..]))
    } else {
        (s_after_p, None)
    };

    // Date components (Y, M, W, D)
    let date_components = parse_components(date_part, &['Y', 'M', 'W', 'D'], original_input)?;

    // Time components (H, M, S)
    let time_components = if let Some(time_str) = time_part {
        let comps = parse_components(time_str, &['H', 'M', 'S'], original_input)?;
        if comps.is_empty() {
            bail!(
                "Time part present but no time components in: {}",
                original_input
            );
        }
        comps
    } else {
        vec![]
    };

    if date_components.is_empty() && time_components.is_empty() {
        bail!("No components in duration: {}", original_input);
    }

    // Accumulate date duration
    let date_duration =
        date_components
            .iter()
            .fold(Duration::zero(), |acc, &(num, unit)| match unit {
                'Y' => acc + Duration::days(num * 365),
                'M' => acc + Duration::days(num * 30),
                'W' => acc + Duration::days(num * 7),
                'D' => acc + Duration::days(num),
                _ => unreachable!("Invalid date unit should be caught by prior validation"),
            });

    // Accumulate time duration
    let time_duration =
        time_components
            .iter()
            .fold(Duration::zero(), |acc, &(num, unit)| match unit {
                'H' => acc + Duration::hours(num),
                'M' => acc + Duration::minutes(num),
                'S' => acc + Duration::seconds(num),
                _ => unreachable!("Invalid time unit should be caught by prior validation"),
            });

    let mut total = date_duration + time_duration;
    if is_negative {
        total = -total;
    }

    Ok(total)
}

/// Parses a human-readable duration string into a `chrono::Duration`.
fn parse_human_readable_duration(s: &str, original_input: &str) -> Result<Duration> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() || parts.len() % 2 != 0 {
        bail!(
            "Invalid human-readable duration format in: {}",
            original_input
        );
    }

    let durations: Result<Vec<Duration>> = parts
        .chunks(2)
        .map(|chunk| {
            let num: i64 = chunk[0]
                .parse()
                .map_err(|_| anyhow!("Invalid number '{}' in: {}", chunk[0], original_input))?;

            match chunk[1].to_lowercase().as_str() {
                "day" | "days" => Ok(Duration::days(num)),
                "hour" | "hours" => Ok(Duration::hours(num)),
                "minute" | "minutes" => Ok(Duration::minutes(num)),
                "second" | "seconds" => Ok(Duration::seconds(num)),
                "millisecond" | "milliseconds" => Ok(Duration::milliseconds(num)),
                "microsecond" | "microseconds" => Ok(Duration::microseconds(num)),
                _ => bail!("Invalid unit '{}' in: {}", chunk[1], original_input),
            }
        })
        .collect();

    durations.map(|durs| durs.into_iter().sum())
}

/// Parses a duration string into a `chrono::Duration`, trying ISO 8601 first, then human-readable format.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let original_input = s;
    let s = s.trim();
    if s.is_empty() {
        bail!("Empty duration string");
    }

    let is_likely_iso8601 = match s.as_bytes() {
        [c, ..] if c.eq_ignore_ascii_case(&b'P') => true,
        [b'-', c, ..] if c.eq_ignore_ascii_case(&b'P') => true,
        _ => false,
    };

    if is_likely_iso8601 {
        parse_iso8601_duration(s, original_input)
    } else {
        parse_human_readable_duration(s, original_input)
    }
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
        check_err_contains(parse_duration("P"), "No components in duration: P", "\"P\"");
    }

    #[test]
    fn test_iso_pt() {
        check_err_contains(
            parse_duration("PT"),
            "Time part present but no time components in: PT",
            "\"PT\"",
        );
    }

    #[test]
    fn test_iso_missing_number_before_unit_in_date_part() {
        check_err_contains(parse_duration("PD"), "Expected number in: PD", "\"PD\"");
    }
    #[test]
    fn test_iso_missing_number_before_unit_in_time_part() {
        check_err_contains(parse_duration("PTM"), "Expected number in: PTM", "\"PTM\"");
    }

    #[test]
    fn test_iso_time_unit_without_t() {
        check_err_contains(parse_duration("P1H"), "Invalid unit 'H' in: P1H", "\"P1H\"");
        check_err_contains(parse_duration("P1S"), "Invalid unit 'S' in: P1S", "\"P1S\"");
    }

    #[test]
    fn test_iso_invalid_number_parse() {
        check_err_contains(
            parse_duration("PT99999999999999999999H"),
            "Invalid number '99999999999999999999' in: PT99999999999999999999H",
            "\"PT99999999999999999999H\"",
        );
    }

    #[test]
    fn test_iso_invalid_unit() {
        check_err_contains(parse_duration("P1X"), "Invalid unit 'X' in: P1X", "\"P1X\"");
        check_err_contains(
            parse_duration("PT1X"),
            "Invalid unit 'X' in: PT1X",
            "\"PT1X\"",
        );
    }

    #[test]
    fn test_iso_valid_lowercase_unit_is_not_allowed() {
        check_err_contains(
            parse_duration("p1h"),
            "Duration must start with 'P' in: p1h",
            "\"p1h\"",
        );
        check_err_contains(
            parse_duration("PT1h"),
            "Invalid unit 'h' in: PT1h",
            "\"PT1h\"",
        );
    }

    #[test]
    fn test_iso_trailing_number_error() {
        check_err_contains(
            parse_duration("P1D2"),
            "Missing unit after number '2' in: P1D2",
            "\"P1D2\"",
        );
    }

    #[test]
    fn test_iso_trailing_number_without_unit_after_p() {
        check_err_contains(
            parse_duration("P1"),
            "Missing unit after number '1' in: P1",
            "\"P1\"",
        );
    }

    #[test]
    fn test_iso_fractional_seconds_fail() {
        check_err_contains(
            parse_duration("PT1.5S"),
            "Invalid unit '.' in: PT1.5S",
            "\"PT1.5S\"",
        );
    }

    #[test]
    fn test_iso_misplaced_t() {
        check_err_contains(
            parse_duration("P1DT2H T3M"),
            "Expected number in: P1DT2H T3M",
            "\"P1DT2H T3M\"",
        );
        check_err_contains(
            parse_duration("P1T2H"),
            "Missing unit after number '1' in: P1T2H",
            "\"P1T2H\"",
        );
    }

    #[test]
    fn test_iso_negative_number_after_p() {
        check_err_contains(
            parse_duration("P-1D"),
            "Expected number in: P-1D",
            "\"P-1D\"",
        );
    }

    #[test]
    fn test_iso_valid_months() {
        check_ok(parse_duration("P1M"), Duration::days(30), "\"P1M\"");
        check_ok(parse_duration(" P13M"), Duration::days(13 * 30), "\"P13M\"");
    }

    #[test]
    fn test_iso_valid_weeks() {
        check_ok(parse_duration("P1W"), Duration::days(7), "\"P1W\"");
        check_ok(parse_duration("      P1W "), Duration::days(7), "\"P1W\"");
    }

    #[test]
    fn test_iso_valid_days() {
        check_ok(parse_duration("P1D"), Duration::days(1), "\"P1D\"");
    }

    #[test]
    fn test_iso_valid_hours() {
        check_ok(parse_duration("PT2H"), Duration::hours(2), "\"PT2H\"");
    }

    #[test]
    fn test_iso_valid_minutes() {
        check_ok(parse_duration("PT3M"), Duration::minutes(3), "\"PT3M\"");
    }

    #[test]
    fn test_iso_valid_seconds() {
        check_ok(parse_duration("PT4S"), Duration::seconds(4), "\"PT4S\"");
    }

    #[test]
    fn test_iso_combined_units() {
        check_ok(
            parse_duration("P1Y2M3W4DT5H6M7S"),
            Duration::days(365 + 60 + 3 * 7 + 4)
                + Duration::hours(5)
                + Duration::minutes(6)
                + Duration::seconds(7),
            "\"P1Y2M3DT4H5M6S\"",
        );
        check_ok(
            parse_duration("P1DT2H3M4S"),
            Duration::days(1) + Duration::hours(2) + Duration::minutes(3) + Duration::seconds(4),
            "\"P1DT2H3M4S\"",
        );
    }

    #[test]
    fn test_iso_duplicated_unit() {
        check_ok(parse_duration("P1D1D"), Duration::days(2), "\"P1D1D\"");
        check_ok(parse_duration("PT1H1H"), Duration::hours(2), "\"PT1H1H\"");
    }

    #[test]
    fn test_iso_out_of_order_unit() {
        check_ok(
            parse_duration("P1W1Y"),
            Duration::days(365 + 7),
            "\"P1W1Y\"",
        );
        check_ok(
            parse_duration("PT2S1H"),
            Duration::hours(1) + Duration::seconds(2),
            "\"PT2S1H\"",
        );
        check_ok(parse_duration("P3M"), Duration::days(90), "\"PT2S1H\"");
        check_ok(parse_duration("PT3M"), Duration::minutes(3), "\"PT2S1H\"");
        check_err_contains(
            parse_duration("P1H2D"),
            "Invalid unit 'H' in: P1H2D", // Time part without 'T' is invalid
            "\"P1H2D\"",
        );
    }

    #[test]
    fn test_iso_negative_duration_p1d() {
        check_ok(parse_duration("-P1D"), -Duration::days(1), "\"-P1D\"");
    }

    #[test]
    fn test_iso_zero_duration_pd0() {
        check_ok(parse_duration("P0D"), Duration::zero(), "\"P0D\"");
    }

    #[test]
    fn test_iso_zero_duration_pt0s() {
        check_ok(parse_duration("PT0S"), Duration::zero(), "\"PT0S\"");
    }

    #[test]
    fn test_iso_zero_duration_pt0h0m0s() {
        check_ok(parse_duration("PT0H0M0S"), Duration::zero(), "\"PT0H0M0S\"");
    }

    // Human-readable Tests
    #[test]
    fn test_human_missing_unit() {
        check_err_contains(
            parse_duration("1"),
            "Invalid human-readable duration format in: 1",
            "\"1\"",
        );
    }

    #[test]
    fn test_human_missing_number() {
        check_err_contains(
            parse_duration("day"),
            "Invalid human-readable duration format in: day",
            "\"day\"",
        );
    }

    #[test]
    fn test_human_incomplete_pair() {
        check_err_contains(
            parse_duration("1 day 2"),
            "Invalid human-readable duration format in: 1 day 2",
            "\"1 day 2\"",
        );
    }

    #[test]
    fn test_human_invalid_number_at_start() {
        check_err_contains(
            parse_duration("one day"),
            "Invalid number 'one' in: one day",
            "\"one day\"",
        );
    }

    #[test]
    fn test_human_invalid_unit() {
        check_err_contains(
            parse_duration("1 hour 2 minutes 3 seconds four seconds"),
            "Invalid number 'four' in: 1 hour 2 minutes 3 seconds four seconds",
            "\"1 hour 2 minutes 3 seconds four seconds\"",
        );
    }

    #[test]
    fn test_human_float_number_fail() {
        check_err_contains(
            parse_duration("1.5 hours"),
            "Invalid number '1.5' in: 1.5 hours",
            "\"1.5 hours\"",
        );
    }

    #[test]
    fn test_invalid_human_readable_no_pairs() {
        check_err_contains(
            parse_duration("just some words"),
            "Invalid human-readable duration format in: just some words",
            "\"just some words\"",
        );
    }

    #[test]
    fn test_human_unknown_unit() {
        check_err_contains(
            parse_duration("1 year"),
            "Invalid unit 'year' in: 1 year",
            "\"1 year\"",
        );
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
    fn test_human_combined() {
        let expected =
            Duration::days(1) + Duration::hours(2) + Duration::minutes(3) + Duration::seconds(4);
        check_ok(
            parse_duration("1 day 2 hours 3 minutes 4 seconds"),
            expected,
            "\"1 day 2 hours 3 minutes 4 seconds\"",
        );
    }

    #[test]
    fn test_human_out_of_order() {
        check_ok(
            parse_duration("1 second 2 hours"),
            Duration::hours(2) + Duration::seconds(1),
            "\"1 second 2 hours\"",
        );
        check_ok(
            parse_duration("7 minutes 6 hours 5 days"),
            Duration::days(5) + Duration::hours(6) + Duration::minutes(7),
            "\"7 minutes 6 hours 5 days\"",
        )
    }

    #[test]
    fn test_human_zero_duration_seconds() {
        check_ok(
            parse_duration("0 seconds"),
            Duration::zero(),
            "\"0 seconds\"",
        );
    }

    #[test]
    fn test_human_zero_duration_days_hours() {
        check_ok(
            parse_duration("0 day 0 hour"),
            Duration::zero(),
            "\"0 day 0 hour\"",
        );
    }

    #[test]
    fn test_human_zero_duration_multiple_zeros() {
        check_ok(
            parse_duration("0 days 0 hours 0 minutes 0 seconds"),
            Duration::zero(),
            "\"0 days 0 hours 0 minutes 0 seconds\"",
        );
    }

    #[test]
    fn test_human_no_space_between_num_unit() {
        check_err_contains(
            parse_duration("1day"),
            "Invalid human-readable duration format in: 1day",
            "\"1day\"",
        );
    }

    #[test]
    fn test_human_trimmed() {
        check_ok(parse_duration(" 1 day "), Duration::days(1), "\" 1 day \"");
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
}
