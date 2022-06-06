use ansilo_core::err::{bail, Context, Result};
use serde_yaml::{Mapping, Value};

use super::{ConfigStringExpr, ConfigStringInterpolation};

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
pub fn parse_expressions(str: &str) -> Result<ConfigStringExpr> {
    let mut brackets = 0u16;
    let mut prev = None;
    let mut stack = vec![];
    let mut exp = ConfigStringExpr::Concat(vec![]);

    for (i, c) in str.char_indices() {
        match (prev, c, &mut exp) {
            // escape using \
            (Some(_), '\\', _) => {
                prev = None;
                continue;
            }
            // ignore escaped char
            (None, _, _) => {}
            // start parsing ${...} expression
            (Some('$'), '{', exp) => {
                stack.push(exp);
                *exp = ConfigStringExpr::Interpolation(ConfigStringInterpolation::new(vec![]));
            }
            // end parsing ${...} expressions
            (Some(_), '}', exp) => {
                if let ConfigStringExpr::Interpolation(interpolation) = exp {
                    *exp = stack.pop().unwrap();
                    append_node(exp, ConfigStringExpr::Interpolation(interpolation));
                } else {
                    bail!("Failed to parse ${{...}} expression, could not match closing bracket in string \"{}\"", str);
                }
            }
            // handle ':' seperator in ${...} expressions
            (Some(_), ':', ConfigStringExpr::Interpolation(ref mut interpolation)) => {
                interpolation.parts.push(ConfigStringExpr::Concat(vec![]))
            }
            // append current char to expression
            (Some(_), c, exp) => {
                append_char(exp, c);
            }
        }

        prev = Some(c);
    }

    // TODO: simply unneccessary nodes
    todo!()
}

/// allocate a reasonable buffer for each new string
fn alloc_string(c: Option<char>) -> String {
    let s = String::with_capacity(64);
    if let Some(c) = c {
        s.push(c);
    }
    s
}

fn append_node(exp: &mut ConfigStringExpr, interpolation: ConfigStringExpr) {
    todo!()
}

fn append_char(exp: &mut ConfigStringExpr, c: char) {
    match exp {
        ConfigStringExpr::Constant(ref mut str) => str.push(c),
        ConfigStringExpr::Interpolation(ref mut interpolation) => {
            if let Some(p) = interpolation.parts.last_mut() {
                append_char(p, c);
            } else {
                interpolation.parts.push(alloc_string(Some(c)))
            }
        }
        ConfigStringExpr::Concat(ref mut parts) => {
            if let Some(ref mut p) = parts.last() {
                append_char(p, c)
            } else {
                parts.push(ConfigStringExpr::Constant(alloc_string(Some(c))))
            }
        }
    }
}
