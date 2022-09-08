use tokio_postgres::types::{private::BytesMut, to_sql_checked, FromSql, IsNull, ToSql, Type};

/// Conversion of strings which also includes the 'tid' type for 'ctid' row id's
#[derive(Debug)]
pub struct CustomString(pub String);

impl<'a> FromSql<'a> for CustomString {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(String::from_sql(ty, raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::TID => true,
            _ => <String as FromSql>::accepts(ty),
        }
    }
}

impl ToSql for CustomString {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        Ok(String::to_sql(&self.0, ty, out)?)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        match *ty {
            Type::TID => true,
            _ => <String as ToSql>::accepts(ty),
        }
    }

    to_sql_checked!();
}
