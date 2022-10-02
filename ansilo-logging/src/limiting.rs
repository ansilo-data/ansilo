use std::fmt::{self, Debug};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaxLogLength<'a, T: Debug> {
    limit: Option<usize>,
    val: &'a T,
}

impl<'a, T: Debug> MaxLogLength<'a, T> {
    pub fn new(limit: Option<usize>, val: &'a T) -> Self {
        Self { limit, val }
    }
}

impl<'a, T: Debug> Debug for MaxLogLength<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fmt = format!("{:?}", self.val);
        if self.limit.is_some() && fmt.len() > self.limit.unwrap() {
            write!(f, "{}", &fmt[..self.limit.unwrap()])?;
            write!(f, "...")?;
        } else {
            write!(f, "{}", fmt)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_log_length_within_bounds() {
        let val = vec![1, 2, 3, 4, 5];
        let fmt = format!("{:?}", MaxLogLength::new(Some(50), &val));

        assert_eq!(fmt, "[1, 2, 3, 4, 5]");
    }

    #[test]
    fn test_max_log_length_no_limit() {
        let val = vec![1, 2, 3, 4, 5];
        let fmt = format!("{:?}", MaxLogLength::new(None, &val));

        assert_eq!(fmt, "[1, 2, 3, 4, 5]");
    }

    #[test]
    fn test_max_log_length_truncated() {
        let val = vec![1, 2, 3, 4, 5];
        let fmt = format!("{:?}", MaxLogLength::new(Some(5), &val));

        assert_eq!(fmt, "[1, 2...");
    }
}
