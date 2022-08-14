use bincode::{
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
    Decode, Encode,
};

use super::EntitySourceConfig;

impl Encode for EntitySourceConfig {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.data_source_id.encode(encoder)?;
        serde_yaml::to_string(&self.options)
            .map_err(|e| EncodeError::OtherString(e.to_string()))?
            .encode(encoder)?;
        Ok(())
    }
}

impl Decode for EntitySourceConfig {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            data_source_id: String::decode(decoder)?,
            options: serde_yaml::from_str(&String::decode(decoder)?)
                .map_err(|e| DecodeError::OtherString(e.to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn test_bincode_entity_source_config_minimal() {
        let config = bincode::config::standard();
        let data = EntitySourceConfig::minimal("test");

        let encoded = bincode::encode_to_vec(data.clone(), config).unwrap();
        assert_eq!(
            bincode::decode_from_std_read::<EntitySourceConfig, _, _>(
                &mut io::Cursor::new(encoded),
                config
            )
            .unwrap(),
            data
        );
    }

    #[test]
    fn test_bincode_entity_source_config_with_yaml() {
        let config = bincode::config::standard();
        let data = EntitySourceConfig {
            data_source_id: "test".into(),
            options: serde_yaml::from_str("a: b\nc: d\n").unwrap(),
        };

        let encoded = bincode::encode_to_vec(data.clone(), config).unwrap();
        assert_eq!(
            bincode::decode_from_std_read::<EntitySourceConfig, _, _>(
                &mut io::Cursor::new(encoded),
                config
            )
            .unwrap(),
            data
        );
    }
}
