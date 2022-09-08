use tokio_postgres::types::{private::BytesMut, to_sql_checked, IsNull, ToSql, Type};

/// Conversion of DataValue::Null without explicit type to postgres null
#[derive(Debug)]
pub struct Null;

impl ToSql for Null {
    fn to_sql(
        &self,
        _ty: &Type,
        _w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}
