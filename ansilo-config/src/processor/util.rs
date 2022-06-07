use std::mem;

use ansilo_core::err::{bail, Context, Result};
use serde_yaml::{Mapping, Value};

use super::ConfigStringExpr as X;

/// Recursively walks the configuration nodes uses the supplied callback
/// to transforms any strings found
pub fn process_strings(node: Value, cb: &impl Fn(String) -> Result<String>) -> Result<Value> {
    Ok(match node {
        Value::String(str) => Value::String(
            cb(str.clone()).context(format!("Failed to process config string {}", str))?,
        ),
        Value::Sequence(seq) => Value::Sequence(
            seq.into_iter()
                .map(|n| process_strings(n, cb))
                .collect::<Result<Vec<Value>>>()?,
        ),
        Value::Mapping(map) => Value::Mapping(
            map.into_iter()
                .map(|i| -> Result<(Value, Value)> {
                    Ok((process_strings(i.0, cb)?, process_strings(i.1, cb)?))
                })
                .collect::<Result<Mapping>>()?,
        ),
        n @ _ => n,
    })
}

/// Parse strings into an expression AST
fn parse_expression(str: &str) -> Result<X> {
    #[derive(Debug, Clone, Copy)]
    enum State {
        Consume,
        Escaped,
        Skip,
        Break,
    }

    let mut stack = vec![];
    let mut exp = X::Concat(vec![]);

    let chars = str.chars().collect::<Vec<char>>();
    let mut state = State::Consume;

    for i in 0..chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1);

        match (state, c, next) {
            // escape using \
            (State::Consume, '\\', _) => {
                state = State::Escaped;
            }
            (State::Escaped, c, _) => {
                append_char(&mut exp, c);
                state = State::Consume;
            }
            // start parsing ${...} expression
            (State::Consume | State::Break, '$', Some('{')) => {
                let inner = mem::replace(&mut exp, X::Interpolation(vec![]));
                stack.push(inner);
                state = State::Skip;
            }
            // end parsing ${...} expressions
            (State::Consume | State::Break, '}', _) => {
                if let X::Interpolation(_) = exp {
                    let outer = stack.pop().unwrap();
                    exp = append_node(outer, exp);
                    state = if let Some(':') = next {
                        State::Consume
                    } else {
                        State::Break
                    };
                } else {
                    bail!("Failed to parse ${{...}} expression, could not match closing bracket in string \"{}\"", str);
                }
            }
            // handle ':' seperator in ${...} expressions
            (State::Consume, ':', _) => match exp {
                X::Interpolation(ref mut parts) => {
                    state = State::Break;
                }
                _ => {
                    append_char(&mut exp, c);
                }
            },
            // append current char to expression
            (State::Consume, c, _) => {
                append_char(&mut exp, c);
            }
            // break parts in ${..:..} expression
            (State::Break, c, _) => match exp {
                X::Interpolation(ref mut parts) => {
                    state = if c == ':' {
                        parts.push(X::Constant(alloc_string(None)));
                        State::Break
                    } else {
                        parts.push(X::Constant(alloc_string(Some(c))));
                        State::Consume
                    };
                }
                X::Concat(ref mut parts) => {
                    parts.push(X::Constant(alloc_string(Some(c))));
                    state = State::Consume;
                }
                _ => {
                    append_char(&mut exp, c);
                    state = State::Consume;
                }
            },
            (State::Skip, _, _) => {
                state = State::Consume;
            }
        }
    }

    if !stack.is_empty() {
        bail!(
            "Failed to parse expression \"{}\", found unclosed ${{...}}",
            str
        )
    }

    exp = simplify_node(exp).unwrap_or_else(|| X::Constant(alloc_string(None)));

    Ok(exp)
}

/// allocate a reasonable buffer for each new string
fn alloc_string(c: Option<char>) -> String {
    let mut s = String::with_capacity(64);
    if let Some(c) = c {
        s.push(c);
    }

    s
}

/// Appends the supplied node to exp
fn append_node(exp: X, node: X) -> X {
    match exp {
        c @ X::Constant(_) => X::Concat(vec![c, node]),
        X::Concat(mut parts) => {
            parts.push(node);
            X::Concat(parts)
        }
        X::Interpolation(mut parts) => {
            parts.push(node);
            X::Interpolation(parts)
        }
    }
}

/// Appends the supplied char to exp
fn append_char(exp: &mut X, c: char) {
    match exp {
        X::Constant(ref mut str) => str.push(c),
        X::Interpolation(ref mut parts) => {
            if let Some(p) = parts.last_mut() {
                append_char(p, c);
            } else {
                parts.push(X::Constant(alloc_string(Some(c))))
            }
        }
        X::Concat(ref mut parts) => {
            if let Some(p) = parts.last_mut() {
                append_char(p, c)
            } else {
                parts.push(X::Constant(alloc_string(Some(c))))
            }
        }
    }
}

/// Simplifies the supplied expression node
fn simplify_node(exp: X) -> Option<X> {
    match exp {
        // remove redundant concat nodes
        X::Concat(parts) if parts.len() == 0 => None,
        X::Concat(mut parts) if parts.len() == 1 => simplify_node(parts.pop().unwrap()),
        X::Concat(parts) => Some(X::Concat(simplify_nodes(parts.into_iter()))),
        X::Interpolation(parts) => Some(X::Interpolation(simplify_nodes(parts.into_iter().map(
            |n| match n {
                X::Concat(parts) if parts.len() == 0 => X::Constant(alloc_string(None)),
                _ => n,
            },
        )))),
        _ => Some(exp),
    }
}

/// Simplifies the supplied expression node
fn simplify_nodes(nodes: impl Iterator<Item = X>) -> Vec<X> {
    nodes
        .into_iter()
        .map(simplify_node)
        .filter(|n| n.is_some())
        .map(|i| i.unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_expression_constants() {
        assert_eq!(parse_expression("").unwrap(), X::Constant("".to_string()));
        assert_eq!(
            parse_expression("abc").unwrap(),
            X::Constant("abc".to_string())
        );
        assert_eq!(
            parse_expression("abc:123").unwrap(),
            X::Constant("abc:123".to_string())
        );
    }

    #[test]
    fn test_parse_expression_escaping() {
        assert_eq!(
            parse_expression("test\\escaped").unwrap(),
            X::Constant("testescaped".to_string())
        );
        assert_eq!(
            parse_expression("\\escaped").unwrap(),
            X::Constant("escaped".to_string())
        );
        assert_eq!(
            parse_expression("escaped\\").unwrap(),
            X::Constant("escaped".to_string())
        );
        assert_eq!(
            parse_expression("\\e\\s\\c\\a\\p\\e\\d").unwrap(),
            X::Constant("escaped".to_string())
        );
        assert_eq!(
            parse_expression("\\\\").unwrap(),
            X::Constant("\\".to_string())
        );
        assert_eq!(
            parse_expression("\\\\\\\\").unwrap(),
            X::Constant("\\\\".to_string())
        );
    }

    #[test]
    fn test_parse_expression_interpolation() {
        assert_eq!(parse_expression("${}").unwrap(), X::Interpolation(vec![]));
        assert_eq!(
            parse_expression("${abc}").unwrap(),
            X::Interpolation(vec![X::Constant("abc".to_owned())])
        );
        assert_eq!(
            parse_expression("${abc:def:ghi}").unwrap(),
            X::Interpolation(vec![
                X::Constant("abc".to_owned()),
                X::Constant("def".to_owned()),
                X::Constant("ghi".to_owned())
            ])
        );
        assert_eq!(
            parse_expression("${FOO::BAR}").unwrap(),
            X::Interpolation(vec![
                X::Constant("FOO".to_owned()),
                X::Constant("".to_owned()),
                X::Constant("BAR".to_owned()),
            ])
        );
    }

    #[test]
    fn test_parse_expression_interpolation_nested() {
        assert_eq!(
            parse_expression("${${}}").unwrap(),
            X::Interpolation(vec![X::Interpolation(vec![])])
        );
        assert_eq!(
            parse_expression("${abc:${def}:ghi}").unwrap(),
            X::Interpolation(vec![
                X::Constant("abc".to_owned()),
                X::Interpolation(vec![X::Constant("def".to_owned())]),
                X::Constant("ghi".to_owned())
            ])
        );
    }

    #[test]
    fn test_parse_expression_interpolation_concat() {
        assert_eq!(
            parse_expression("a${b}c${d}").unwrap(),
            X::Concat(vec![
                X::Constant("a".to_owned()),
                X::Interpolation(vec![X::Constant("b".to_owned())]),
                X::Constant("c".to_owned()),
                X::Interpolation(vec![X::Constant("d".to_owned())])
            ])
        );
    }
}
