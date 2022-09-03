/// Outputs the supplied utf8 string as a pg string literal
pub fn pg_str_literal(str: &str) -> String {
    let mut escaped = String::with_capacity(str.len() + 3);
    escaped.push_str("E'");

    for c in str.chars() {
        if c == '\'' || c == '\\' {
            escaped.push(c);
            escaped.push(c);
        } else if c.is_ascii() {
            escaped.push(c);
        } else {
            escaped.push_str("\\U");
            escaped.push_str(&format!("{:08X}", c as u32))
        }
    }

    escaped.push('\'');

    escaped
}

/// Outputs the supplied string a quoted identifier
pub fn pg_quote_identifier(str: &str) -> String {
    let mut escaped = String::with_capacity(str.len() + 2);
    escaped.push('"');

    for c in str.chars() {
        if c == '"' {
            escaped.push(c);
            escaped.push(c);
        } else {
            escaped.push(c);
        }
    }

    escaped.push('"');

    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_str_literal() {
        assert_eq!(pg_str_literal(""), "E''".to_string());
        assert_eq!(pg_str_literal("abc"), "E'abc'".to_string());
        assert_eq!(pg_str_literal("abc'123"), "E'abc''123'".to_string());
        assert_eq!(pg_str_literal("abc\\123"), "E'abc\\\\123'".to_string());
        assert_eq!(pg_str_literal("abc\\'123"), "E'abc\\\\''123'".to_string());
        assert_eq!(pg_str_literal("🥑"), "E'\\U0001F951'".to_string());
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(pg_quote_identifier(""), "\"\"".to_string());
        assert_eq!(pg_quote_identifier("abc"), "\"abc\"".to_string());
        assert_eq!(pg_quote_identifier("abc'123"), "\"abc'123\"".to_string());
        assert_eq!(
            pg_quote_identifier("abc\"123"),
            "\"abc\"\"123\"".to_string()
        );
        assert_eq!(pg_quote_identifier("abc\\123"), "\"abc\\123\"".to_string());
        assert_eq!(pg_quote_identifier("🥑"), "\"🥑\"".to_string());
    }
}
