use arrow_schema::DataType;
use tinystr::TinyAsciiStr;

use crate::deserialize::ArrowDeserialize;
use crate::field::ArrowField;
use crate::serialize::ArrowSerialize;

use arrow_array::{builder::FixedSizeBinaryBuilder, FixedSizeBinaryArray};

impl<const N: usize> ArrowField for TinyAsciiStr<N> {
    type Type = Self;

    fn data_type() -> DataType {
        DataType::FixedSizeBinary(N as i32)
    }
}

impl<const N: usize> ArrowSerialize for TinyAsciiStr<N> {
    type ArrayBuilderType = FixedSizeBinaryBuilder;

    fn new_array() -> Self::ArrayBuilderType {
        FixedSizeBinaryBuilder::new(N as i32)
    }

    fn arrow_serialize(v: &Self::Type, array: &mut Self::ArrayBuilderType) -> Result<(), arrow_schema::ArrowError> {
        array.append_value(v.as_bytes())?;
        Ok(())
    }
}

impl<const N: usize> ArrowDeserialize for TinyAsciiStr<N> {
    type ArrayType = FixedSizeBinaryArray;

    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.and_then(|bytes| TinyAsciiStr::try_from_utf8(bytes).ok())
    }
}
