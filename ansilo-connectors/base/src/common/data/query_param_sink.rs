use std::io::{self, Write};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Error, Result},
};

use crate::{common::query::QueryParam, interface::QueryInputStructure};

use super::DataSink;

/// Captures the query input parameters.
#[derive(Clone)]
pub struct QueryParamSink {
    /// The list of query parameters expected by the query.
    params: Vec<QueryParam>,
    /// The input structure expected by the query.
    /// This only contains the dynamic query params and excludes constants.
    input: QueryInputStructure,
    /// The underlying data sink, used to capture dynamic parameters
    sink: DataSink,
    /// The current DataValue's which have been written to the sink
    /// This will only contain the dynamic query parameters.
    values: Vec<DataValue>,
}

impl QueryParamSink {
    pub fn new(params: Vec<QueryParam>) -> Self {
        let input = QueryInputStructure::new(
            params
                .iter()
                .filter_map(|p| p.as_dynamic().map(|p| p))
                .map(|p| (p.id, p.r#type.clone()))
                .collect(),
        );

        let sink = DataSink::new(input.types());

        Self {
            params,
            input,
            sink,
            values: vec![],
        }
    }

    /// Gets the expected query input structure when writing to the query.
    pub fn get_input_structure(&self) -> &QueryInputStructure {
        &self.input
    }

    /// Gets the list of query parameters
    pub fn get_params(&self) -> &Vec<QueryParam> {
        &self.params
    }

    /// Returns whether all query params have been written
    pub fn all_params_written(&self) -> bool {
        self.values.len() == self.input.params.len()
    }

    /// Returns the query parameter values, including both constants and dynamic parameters
    pub fn get_all(&self) -> Result<Vec<DataValue>> {
        if !self.all_params_written() {
            bail!(
                "Only {}/{} query parameters written",
                self.values.len(),
                self.input.params.len()
            );
        }

        let mut res = vec![];
        let mut dyn_param_idx = 0;

        for param in self.params.iter() {
            match param {
                QueryParam::Dynamic(_) => {
                    res.push(self.values[dyn_param_idx].clone());
                    dyn_param_idx += 1;
                }
                QueryParam::Constant(v) => res.push(v.clone()),
            }
        }

        Ok(res)
    }

    /// Clears the query parameter sink, clearing all current input
    pub fn clear(&mut self) {
        self.values = vec![];
        self.sink.clear();
    }
}

impl Write for QueryParamSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.values.len() == self.input.params.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                Error::msg("All query parameteres have already been written"),
            ));
        }

        let res = self.sink.write(buf)?;

        while !self.all_params_written() {
            let val = self
                .sink
                .read_data_value()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            if val.is_none() {
                break;
            }

            self.values.push(val.unwrap());
        }

        if self.all_params_written() && self.sink.buf_len() > 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                Error::msg("Excess query input data found when all query input parameters written"),
            ));
        }

        Ok(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::sqlil;

    use super::*;

    #[test]
    fn test_query_param_sink_read_new() {
        let sink = QueryParamSink::new(vec![
            QueryParam::Dynamic(sqlil::Parameter::new(DataType::UInt32, 1)),
            QueryParam::Constant(DataValue::Utf8String("hi".into())),
        ]);

        assert_eq!(
            sink.get_input_structure(),
            &QueryInputStructure::new(vec![(1, DataType::UInt32)])
        );

        assert_eq!(sink.all_params_written(), false);
        assert!(sink.get_all().is_err());
    }

    #[test]
    fn test_query_param_sink_empty() {
        let mut sink = QueryParamSink::new(vec![]);

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(sink.get_all().unwrap(), vec![]);
        sink.write(&[1]).unwrap_err();
    }

    #[test]
    fn test_query_param_sink_single_param() {
        let mut sink = QueryParamSink::new(vec![QueryParam::Dynamic(sqlil::Parameter::new(
            DataType::UInt16,
            1,
        ))]);

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                123u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(sink.get_all().unwrap(), vec![DataValue::UInt16(123)]);
    }

    #[test]
    fn test_query_param_sink_param_and_constant() {
        let mut sink = QueryParamSink::new(vec![
            QueryParam::Dynamic(sqlil::Parameter::new(DataType::UInt16, 1)),
            QueryParam::Constant(DataValue::Utf8String("hello".into())),
        ]);

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                456u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(
            sink.get_all().unwrap(),
            vec![
                DataValue::UInt16(456),
                DataValue::Utf8String("hello".into())
            ]
        );
    }

    #[test]
    fn test_query_param_sink_interleaved() {
        let mut sink = QueryParamSink::new(vec![
            QueryParam::Dynamic(sqlil::Parameter::new(DataType::UInt16, 1)),
            QueryParam::Constant(DataValue::Utf8String("hello".into())),
            QueryParam::Dynamic(sqlil::Parameter::new(DataType::UInt32, 2)),
            QueryParam::Constant(DataValue::Utf8String("world".into())),
        ]);

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                456u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                789u32.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(
            sink.get_all().unwrap(),
            vec![
                DataValue::UInt16(456),
                DataValue::Utf8String("hello".into()),
                DataValue::UInt32(789),
                DataValue::Utf8String("world".into())
            ]
        );
    }

    #[test]
    fn test_query_param_sink_write_past_end_fails() {
        let mut sink = QueryParamSink::new(vec![QueryParam::Dynamic(sqlil::Parameter::new(
            DataType::UInt16,
            1,
        ))]);

        sink.write_all(
            &[
                vec![1u8],                     // not null
                456u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        sink.write_all(&[0]).unwrap_err();
    }

    #[test]
    fn test_query_param_sink_write_past_end_in_one_buf_fails() {
        let mut sink = QueryParamSink::new(vec![QueryParam::Dynamic(sqlil::Parameter::new(
            DataType::UInt16,
            1,
        ))]);

        sink.write_all(
            &[
                vec![1u8],                     // not null
                456u16.to_be_bytes().to_vec(), // data
                vec![0u8],                     // extra data
            ]
            .concat(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_query_param_sink_clear() {
        let mut sink = QueryParamSink::new(vec![
            QueryParam::Dynamic(sqlil::Parameter::new(DataType::UInt16, 1)),
            QueryParam::Constant(DataValue::Utf8String("hello".into())),
        ]);

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                456u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(
            sink.get_all().unwrap(),
            vec![
                DataValue::UInt16(456),
                DataValue::Utf8String("hello".into())
            ]
        );

        sink.clear();

        assert_eq!(sink.all_params_written(), false);
        sink.get_all().unwrap_err();

        sink.write_all(
            &[
                vec![1u8],                     // not null
                789u16.to_be_bytes().to_vec(), // data
            ]
            .concat(),
        )
        .unwrap();

        assert_eq!(sink.all_params_written(), true);
        assert_eq!(
            sink.get_all().unwrap(),
            vec![
                DataValue::UInt16(789),
                DataValue::Utf8String("hello".into())
            ]
        );
    }
}
