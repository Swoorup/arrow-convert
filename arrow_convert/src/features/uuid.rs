//! UUID feature module

use std::any::Any;
use std::sync::Arc;

use crate::arrow_enable_vec_for_type;
use crate::deserialize::ArrowDeserialize;
use crate::field::ArrowField;
use crate::serialize::ArrowSerialize;
use crate::serialize::PushNull;
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::ArrayRef;
use arrow_array::FixedSizeBinaryArray;
use arrow_schema::DataType;
use uuid::Uuid;

impl ArrowField for Uuid {
    type Type = Self;

    #[inline]
    fn data_type() -> DataType {
        DataType::FixedSizeBinary(16)
    }

    #[inline]
    fn field(name: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(name, Self::data_type(), Self::is_nullable())
            .with_extension_type(arrow_schema::extension::Uuid)
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
    type ArrayBuilderType = UuidBuilder;

    #[inline]
    fn new_array() -> Self::ArrayBuilderType {
        Self::ArrayBuilderType::default()
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::ArrayBuilderType) -> Result<(), arrow_schema::ArrowError> {
        array.0.append_value(v.as_bytes())
    }
}

/// A builder for Uuid arrays wrapping a FixedSizeBinaryBuilder and providing Default for it.
/// It is required so that we can enable the Vec<Uuid> implementation.
#[derive(Debug)]
pub struct UuidBuilder(FixedSizeBinaryBuilder);
impl Default for UuidBuilder {
    fn default() -> Self {
        Self(FixedSizeBinaryBuilder::new(16))
    }
}
impl arrow_array::builder::ArrayBuilder for UuidBuilder {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.0.finish_cloned())
    }
}
impl PushNull for UuidBuilder {
    fn push_null(&mut self) {
        self.0.append_null();
    }
}

arrow_enable_vec_for_type!(Uuid);
