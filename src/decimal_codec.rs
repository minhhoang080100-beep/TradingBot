//! Exact, string-only decimal serialization for trusted-boundary checkpoints.

pub(crate) fn parse_exact(value: &str) -> Result<rust_decimal::Decimal, &'static str> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(a, b)| (a, Some(b)));
    if value.len() > 64
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction
            .is_some_and(|f| f.is_empty() || f.len() > 28 || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("invalid exact decimal string");
    }
    rust_decimal::Decimal::from_str_exact(value).map_err(|_| "invalid exact decimal string")
}

pub(crate) mod required {
    use rust_decimal::Decimal;
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        value: &Decimal,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Decimal, D::Error> {
        let value = String::deserialize(deserializer)?;
        super::parse_exact(&value).map_err(D::Error::custom)
    }
}

pub(crate) mod optional {
    use rust_decimal::Decimal;
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    // Serde's `with` convention passes a reference to the complete field.
    #[allow(clippy::ref_option)]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<Decimal>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Decimal>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| super::parse_exact(&value).map_err(D::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Fixture {
        #[serde(with = "super::required")]
        amount: Decimal,
        #[serde(with = "super::optional")]
        optional: Option<Decimal>,
    }

    #[test]
    fn string_codec_rejects_numbers_and_precision_loss() -> Result<(), serde_json::Error> {
        let fixture = Fixture {
            amount: Decimal::new(123, 2),
            optional: Some(Decimal::ONE),
        };
        assert_eq!(
            serde_json::from_slice::<Fixture>(&serde_json::to_vec(&fixture)?)?,
            fixture
        );
        assert!(serde_json::from_str::<Fixture>(r#"{"amount":1,"optional":null}"#).is_err());
        assert!(serde_json::from_str::<Fixture>(
            r#"{"amount":"0.12345678901234567890123456789","optional":null}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Fixture>(r#"{"amount":"1","optional":1}"#).is_err());
        Ok(())
    }
}
