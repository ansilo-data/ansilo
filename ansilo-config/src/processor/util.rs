use core::borrow::Borrow;
use std::mem;

use ansilo_core::err::{bail, Context, Result};
use serde_yaml::{Mapping, Value};

use super::{ConfigExprResult, ConfigStringExpr as X};

/// Recursively walks the configuration nodes uses the supplied callback
/// to transforms any strings found
pub(crate) fn process_strings(node: Value, cb: &impl Fn(String) -> Result<Value>) -> Result<Value> {
    Ok(match node {
        Value::String(str) => {
            cb(str.clone()).context(format!("Failed to process config string {}", str))?
        }
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

/// Recursively walks the configuration nodes uses the supplied callback
/// to transforms any strings found
///
/// Importantly, this applies the callback bottom-up, this is importantly for correctly
/// processing nested interpolations, eg ${outer:${inner}}
pub(crate) fn process_expression(
    exp: X,
    cb: &impl Fn(X) -> Result<ConfigExprResult>,
) -> Result<ConfigExprResult> {
    fn process_vec(exp: Vec<X>, cb: &impl Fn(X) -> Result<ConfigExprResult>) -> Result<Vec<X>> {
        exp.into_iter()
            .map(|i| process_expression(i, cb))
            .collect::<Result<Vec<ConfigExprResult>>>()?
            .into_iter()
            .map(|i| match i {
                ConfigExprResult::Expr(exp) => Ok(exp),
                ConfigExprResult::Yaml(_) => bail!("Found yaml embedded inside config expression"),
            })
            .collect::<Result<Vec<X>>>()
    }

    let exp = match exp {
        X::Concat(parts) => X::Concat(process_vec(parts, cb)?),
        X::Interpolation(parts) => X::Interpolation(process_vec(parts, cb)?),
        _ => exp,
    };

    cb(exp)
}

/// Parse strings into an expression AST
pub(crate) fn parse_expression(str: &str) -> Result<X> {
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
                X::Interpolation(_) => {
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

/// Converts the supplied expression back to a string
pub(crate) fn expression_to_string(exp: impl Borrow<X>) -> String {
    match exp.borrow() {
        X::Constant(str) => str.to_owned(),
        X::Concat(parts) => parts
            .into_iter()
            .map(|i| expression_to_string(i))
            .collect::<Vec<String>>()
            .join(""),
        X::Interpolation(parts) => format!(
            "${{{}}}",
            parts
                .into_iter()
                .map(|i| expression_to_string(i))
                .collect::<Vec<String>>()
                .join(":")
        ),
    }
}

/// Matches the supplied expr for '${...}' interpolations and returns the inner '...' part strings
pub(crate) fn match_interpolation(exp: impl Borrow<X>, prefix: &[&str]) -> Option<Vec<String>> {
    let parts = match exp.borrow() {
        X::Interpolation(parts) => parts,
        _ => return None,
    };

    let parts = parts
        .iter()
        .map(expression_to_string)
        .collect::<Vec<String>>();

    if parts.len() < prefix.len() {
        return None;
    }

    if parts[..prefix.len()] != *prefix {
        return None;
    }

    return Some(parts.clone());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_strings() {
        let input = serde_yaml::from_str::<serde_yaml::Value>(
            "
a: foo
b:
  c: bar
d:
 - qux",
        )
        .unwrap();

        let expected = serde_yaml::from_str::<serde_yaml::Value>(
            "
a!: foo!
b!:
  c!: bar!
d!:
 - qux!",
        )
        .unwrap();

        let actual = process_strings(input, &|s| Ok(Value::String(s + "!"))).unwrap();

        assert_eq!(actual, expected);
    }

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

    #[test]
    fn test_expression_to_string() {
        assert_eq!(expression_to_string(X::Constant("abc".to_owned())), "abc");
        assert_eq!(
            expression_to_string(X::Interpolation(vec![
                X::Constant("a".to_owned()),
                X::Constant("b".to_owned()),
            ])),
            "${a:b}"
        );

        assert_eq!(
            expression_to_string(X::Concat(vec![
                X::Constant("a".to_owned()),
                X::Interpolation(vec![X::Constant("b".to_owned())]),
                X::Constant("c".to_owned()),
                X::Interpolation(vec![X::Constant("d".to_owned())])
            ])),
            "a${b}c${d}"
        );

        assert_eq!(
            expression_to_string(X::Interpolation(vec![X::Interpolation(vec![X::Constant(
                "abc".to_owned()
            )]),])),
            "${${abc}}"
        );
    }

    #[test]
    fn test_match_interpolation() {
        assert_eq!(
            match_interpolation(X::Constant("a".to_owned()), &["abc"]),
            None
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![X::Constant("a".to_owned())]),
                &["abc"]
            ),
            None
        );
        assert_eq!(
            match_interpolation(X::Interpolation(vec![X::Constant("a".to_owned())]), &[]),
            Some(vec!["a".to_string()])
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![X::Constant("abc".to_owned())]),
                &["abc"]
            ),
            Some(vec!["abc".to_string()])
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![X::Constant("a".to_owned())]),
                &["a", "b", "c"]
            ),
            None
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![
                    X::Constant("a".to_owned()),
                    X::Constant("b".to_owned()),
                    X::Constant("c".to_owned())
                ]),
                &["a", "b"]
            ),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![X::Interpolation(vec![X::Constant("a".to_owned())]),]),
                &[]
            ),
            Some(vec!["${a}".to_string()])
        );
        assert_eq!(
            match_interpolation(
                X::Interpolation(vec![X::Interpolation(vec![X::Constant("a".to_owned())]),]),
                &["${a}"]
            ),
            Some(vec!["${a}".to_string()])
        );
    }
}
