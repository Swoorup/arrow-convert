use crate::deserialize::ArrowDeserialize;
use crate::field::ArrowField;
use crate::serialize::ArrowSerialize;
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::FixedSizeBinaryArray;
use arrow_schema::DataType;
use uuid::Uuid;

impl ArrowField for Uuid {
    type Type = Self;

    #[inline]
    fn data_type() -> DataType {
        DataType::FixedSizeBinary(16)
    }
}

impl ArrowDeserialize for Uuid {
    type ArrayType = FixedSizeBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.and_then(|t| Uuid::from_slice(t).ok())
    }
}

impl ArrowSerialize for Uuid {
    type ArrayBuilderType = FixedSizeBinaryBuilder;

    #[inline]
    fn new_array() -> Self::ArrayBuilderType {
        Self::ArrayBuilderType::new(16)
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::ArrayBuilderType) -> Result<(), arrow_schema::ArrowError> {
        array.append_value(v.as_bytes())
    }
}
