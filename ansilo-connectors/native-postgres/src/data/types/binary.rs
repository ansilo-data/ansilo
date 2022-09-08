use tokio_postgres::types::{private::BytesMut, to_sql_checked, FromSql, IsNull, ToSql, Type};

/// Conversion of binary types which also includes the 'tid' type for 'ctid' row id's
#[derive(Debug)]
pub struct Binary(pub Vec<u8>);

impl<'a> FromSql<'a> for Binary {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(Vec::<u8>::from_sql(ty, raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::TID => true,
            _ => <Vec<u8> as FromSql>::accepts(ty),
        }
    }
}

impl ToSql for Binary {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        Ok(Vec::<u8>::to_sql(&self.0, ty, out)?)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        match *ty {
            Type::TID => true,
            _ => <Vec<u8> as ToSql>::accepts(ty),
        }
    }

    to_sql_checked!();
}
