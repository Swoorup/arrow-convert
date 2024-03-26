//! Implementation and traits for deserializing from Arrow.

use std::sync::Arc;

use arrow::{
    array::*,
    buffer::{Buffer, ScalarBuffer},
    datatypes::{self, ArrowNativeType, ArrowPrimitiveType, Decimal128Type},
};
use chrono::{NaiveDate, NaiveDateTime};

use crate::field::*;

/// Implement by Arrow arrays that can be converted to an iterator
pub trait IntoArrowArrayIterator {
    /// The type of the iterator item
    type Item;
    /// The type of the iterator
    type IntoIter: Iterator<Item = Self::Item>;
    /// Convert the array to an iterator
    fn into_iter(self) -> Self::IntoIter;
}

impl<'a, T: ArrowPrimitiveType> IntoArrowArrayIterator for &'a PrimitiveArray<T> {
    type Item = Option<T::Native>;

    type IntoIter = PrimitiveIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        <Self as IntoIterator>::into_iter(self)
    }
}

/// Implemented by [`ArrowField`] that can be deserialized from arrow
pub trait ArrowDeserialize: ArrowField + Sized
where
    Self::ArrayType: ArrowArray,
    for<'a> &'a Self::ArrayType: IntoArrowArrayIterator,
{
    /// The `arrow::Array` type corresponding to this field
    type ArrayType;

    /// Deserialize this field from arrow
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoArrowArrayIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type>;

    #[inline]
    #[doc(hidden)]
    /// For internal use only
    ///
    /// This is an ugly hack to allow generating a blanket Option<T> deserialize.
    /// Ideally we would be able to capture the optional field of the iterator via
    /// something like for<'a> &'a T::ArrayType: IntoArrowArrayIterator<Item=Option<E>>,
    /// However, the E parameter seems to confuse the borrow checker if it's a reference.
    fn arrow_deserialize_internal(
        v: <&Self::ArrayType as IntoArrowArrayIterator>::Item,
    ) -> <Self as ArrowField>::Type {
        Self::arrow_deserialize(v).unwrap()
    }
}

/// Internal trait used to support deserialization and iteration of structs, and nested struct lists
///
/// Trivial pass-thru implementations are provided for arrow arrays that implement IntoArrowArrayIterator.
///
/// The derive macro generates implementations for typed struct arrays.
#[doc(hidden)]
pub trait ArrowArray
where
    for<'a> &'a Self: IntoArrowArrayIterator,
{
    type BaseArrayType: Array;

    // Returns a typed iterator to the underlying elements of the array from an untyped Array reference.
    fn iter_from_array_ref(b: &dyn Array) -> <&Self as IntoArrowArrayIterator>::IntoIter;
}

// Macro to facilitate implementation for numeric types and numeric arrays.
macro_rules! impl_arrow_deserialize_primitive {
    ($physical_type:ty, $primitive_type:ty) => {
        impl ArrowDeserialize for $physical_type {
            type ArrayType = PrimitiveArray<$primitive_type>;

            #[inline]
            fn arrow_deserialize<'a>(
                v: Option<<$primitive_type as ArrowPrimitiveType>::Native>,
            ) -> Option<Self> {
                v
            }
        }

        impl_arrow_array!(PrimitiveArray<$primitive_type>);
    };
}

macro_rules! impl_arrow_array {
    ($array:ty) => {
        impl ArrowArray for $array {
            type BaseArrayType = Self;

            #[inline]
            fn iter_from_array_ref(b: &dyn Array) -> <&Self as IntoArrowArrayIterator>::IntoIter {
                let b = b.as_any().downcast_ref::<Self::BaseArrayType>().unwrap();
                <&Self as IntoArrowArrayIterator>::into_iter(b)
            }
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowDeserialize for Option<T>
where
    T: ArrowDeserialize,
    T::ArrayType: 'static + ArrowArray,
    for<'a> &'a T::ArrayType: IntoArrowArrayIterator,
{
    type ArrayType = <T as ArrowDeserialize>::ArrayType;

    #[inline]
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoArrowArrayIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        Self::arrow_deserialize_internal(v).map(Some)
    }

    #[inline]
    fn arrow_deserialize_internal(
        v: <&Self::ArrayType as IntoArrowArrayIterator>::Item,
    ) -> <Self as ArrowField>::Type {
        <T as ArrowDeserialize>::arrow_deserialize(v)
    }
}

impl_arrow_deserialize_primitive!(u8, datatypes::UInt8Type);
impl_arrow_deserialize_primitive!(u16, datatypes::UInt16Type);
impl_arrow_deserialize_primitive!(u32, datatypes::UInt32Type);
impl_arrow_deserialize_primitive!(u64, datatypes::UInt64Type);
impl_arrow_deserialize_primitive!(i8, datatypes::Int8Type);
impl_arrow_deserialize_primitive!(i16, datatypes::Int16Type);
impl_arrow_deserialize_primitive!(i32, datatypes::Int32Type);
impl_arrow_deserialize_primitive!(i64, datatypes::Int64Type);
impl_arrow_deserialize_primitive!(half::f16, datatypes::Float16Type);
impl_arrow_deserialize_primitive!(f32, datatypes::Float32Type);
impl_arrow_deserialize_primitive!(f64, datatypes::Float64Type);

impl<const PRECISION: u8, const SCALE: i8> ArrowDeserialize for I128<PRECISION, SCALE> {
    type ArrayType = PrimitiveArray<Decimal128Type>;

    #[inline]
    fn arrow_deserialize<'a>(v: Option<i128>) -> Option<i128> {
        v
    }
}

impl_arrow_array!(PrimitiveArray<Decimal128Type>);

impl<'a, OffsetSize: OffsetSizeTrait> IntoArrowArrayIterator
    for &'a GenericStringArray<OffsetSize>
{
    type Item = Option<&'a str>;

    type IntoIter = ArrayIter<&'a GenericStringArray<OffsetSize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl ArrowDeserialize for String {
    type ArrayType = StringArray;

    #[inline]
    fn arrow_deserialize(v: Option<&str>) -> Option<Self> {
        v.map(|t| t.to_string())
    }
}

impl ArrowDeserialize for LargeString {
    type ArrayType = LargeStringArray;

    #[inline]
    fn arrow_deserialize(v: Option<&str>) -> Option<String> {
        v.map(|t| t.to_string())
    }
}

impl<'a> IntoArrowArrayIterator for &'a BooleanArray {
    type Item = Option<bool>;

    type IntoIter = BooleanIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ArrowDeserialize for bool {
    type ArrayType = BooleanArray;

    #[inline]
    fn arrow_deserialize(v: Option<bool>) -> Option<Self> {
        v
    }
}

impl ArrowDeserialize for NaiveDateTime {
    type ArrayType = TimestampNanosecondArray;

    #[inline]
    fn arrow_deserialize(v: Option<i64>) -> Option<Self> {
        v.and_then(arrow::temporal_conversions::timestamp_ns_to_datetime)
    }
}

impl ArrowDeserialize for NaiveDate {
    type ArrayType = Date32Array;

    #[inline]
    fn arrow_deserialize(v: Option<i32>) -> Option<Self> {
        v.and_then(|t| arrow::temporal_conversions::as_date::<datatypes::Date32Type>(t as i64))
    }
}

/// Iterator for for [`BufferBinaryArray`]
pub struct BufferBinaryArrayIter<'a> {
    index: usize,
    array: &'a BinaryArray,
}

impl<'a> Iterator for BufferBinaryArrayIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else if self.array.is_valid(self.index) {
            // self.array.iter
            let value = self.array.value(self.index);
            self.index += 1;
            Some(Some(value))
        } else {
            self.index += 1;
            Some(None)
        }
    }
}

/// Internal `ArrowArray` helper to iterate over a `BinaryArray` while exposing Buffer slices
pub struct BufferBinaryArray;

impl<'a> IntoArrowArrayIterator for &'a BufferBinaryArray {
    type Item = Option<&'a [u8]>;

    type IntoIter = BufferBinaryArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        unimplemented!("Use iter_from_array_ref");
    }
}

impl ArrowArray for BufferBinaryArray {
    type BaseArrayType = BinaryArray;
    #[inline]
    fn iter_from_array_ref(a: &dyn Array) -> <&Self as IntoArrowArrayIterator>::IntoIter {
        let b = a.as_any().downcast_ref::<Self::BaseArrayType>().unwrap();

        BufferBinaryArrayIter { index: 0, array: b }
    }
}

// Treat both Buffer and ScalarBuffer<u8> the same
impl ArrowDeserialize for Buffer {
    type ArrayType = BufferBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.map(|t| t.into())
    }
}
impl ArrowDeserialize for ScalarBuffer<u8> {
    type ArrayType = BufferBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.map(|t| ScalarBuffer::from(t.to_vec()))
    }
}

impl ArrowDeserialize for Vec<u8> {
    type ArrayType = BinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.map(|t| t.to_vec())
    }
}

impl<'a, OffsetSize: OffsetSizeTrait> IntoArrowArrayIterator
    for &'a GenericBinaryArray<OffsetSize>
{
    type Item = Option<&'a [u8]>;

    type IntoIter = ArrayIter<&'a GenericBinaryArray<OffsetSize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoArrowArrayIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;

    type IntoIter = FixedSizeBinaryIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ArrowDeserialize for LargeBinary {
    type ArrayType = LargeBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Vec<u8>> {
        v.map(|t| t.to_vec())
    }
}

impl<const SIZE: i32> ArrowDeserialize for FixedSizeBinary<SIZE> {
    type ArrayType = FixedSizeBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Vec<u8>> {
        v.map(|t| t.to_vec())
    }
}

fn arrow_deserialize_vec_helper<T>(
    v: Option<Arc<dyn Array>>,
) -> Option<<Vec<T> as ArrowField>::Type>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    for<'a> &'a T::ArrayType: IntoArrowArrayIterator,
{
    use std::ops::Deref;
    v.map(|t| {
        arrow_array_deserialize_iterator_internal::<<T as ArrowField>::Type, T>(t.deref())
            .collect::<Vec<<T as ArrowField>::Type>>()
    })
}

// Blanket implementation for ScalarBuffer
impl<T, K> ArrowDeserialize for ScalarBuffer<T>
where
    K: ArrowPrimitiveType<Native = T>,
    T: ArrowDeserialize<ArrayType = PrimitiveArray<K>> + ArrowNativeType + ArrowEnableVecForType,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    type ArrayType = ListArray;

    #[inline]
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoArrowArrayIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        let t = v?;
        let array = t
            .as_any()
            .downcast_ref::<PrimitiveArray<K>>()
            .unwrap()
            .values()
            .clone();
        Some(array)
    }
}

// Blanket implementation for Vec
impl<T> ArrowDeserialize for Vec<T>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    type ArrayType = ListArray;

    fn arrow_deserialize(v: Option<Arc<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}

impl<'a, OffsetSize: OffsetSizeTrait> IntoArrowArrayIterator for &'a GenericListArray<OffsetSize> {
    type Item = Option<Arc<dyn Array>>;

    type IntoIter = GenericListArrayIter<'a, OffsetSize>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoArrowArrayIterator for &'a FixedSizeListArray {
    type Item = Option<Arc<dyn Array>>;

    type IntoIter = FixedSizeListIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> ArrowDeserialize for LargeVec<T>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    type ArrayType = LargeListArray;

    fn arrow_deserialize(v: Option<Arc<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}

impl<T, const SIZE: i32> ArrowDeserialize for FixedSizeVec<T, SIZE>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    type ArrayType = FixedSizeListArray;

    fn arrow_deserialize(v: Option<Arc<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}

impl_arrow_array!(BooleanArray);
impl_arrow_array!(StringArray);
impl_arrow_array!(LargeStringArray);
impl_arrow_array!(BinaryArray);
impl_arrow_array!(LargeBinaryArray);
impl_arrow_array!(FixedSizeBinaryArray);
impl_arrow_array!(ListArray);
impl_arrow_array!(LargeListArray);
impl_arrow_array!(FixedSizeListArray);
impl_arrow_array!(Date32Array);
impl_arrow_array!(TimestampNanosecondArray);

/// Top-level API to deserialize from Arrow
pub trait TryIntoCollection<Collection, Element>
where
    Element: ArrowField,
    Collection: FromIterator<Element>,
{
    /// Convert from a `arrow::Array` to any collection that implements the `FromIterator` trait
    fn try_into_collection(self) -> arrow::error::Result<Collection>;

    /// Same as `try_into_collection` except can coerce the conversion to a specific Arrow type. This is
    /// useful when the same rust type maps to one or more Arrow types for example `LargeString`.
    fn try_into_collection_as_type<ArrowType>(self) -> arrow::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
        for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator;
}

/// Helper to return an iterator for elements from a [`arrow::array::Array`].
fn arrow_array_deserialize_iterator_internal<'a, Element, Field>(
    b: &'a dyn arrow::array::Array,
) -> impl Iterator<Item = Element> + 'a
where
    Field: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <Field as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    <<Field as ArrowDeserialize>::ArrayType as ArrowArray>::iter_from_array_ref(b)
        .map(<Field as ArrowDeserialize>::arrow_deserialize_internal)
}

/// Returns a typed iterator to a target type from an `arrow::Array`
pub fn arrow_array_deserialize_iterator_as_type<'a, Element, ArrowType>(
    arr: &'a dyn arrow::array::Array,
) -> arrow::error::Result<impl Iterator<Item = Element> + 'a>
where
    Element: 'static,
    ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    if &<ArrowType as ArrowField>::data_type() != arr.data_type() {
        Err(arrow::error::ArrowError::InvalidArgumentError(
            "Data type mismatch".to_string(),
        ))
    } else {
        Ok(arrow_array_deserialize_iterator_internal::<
            Element,
            ArrowType,
        >(arr))
    }
}

/// Return an iterator that deserializes an [`Array`] to an element of type T
pub fn arrow_array_deserialize_iterator<'a, T>(
    arr: &'a dyn arrow::array::Array,
) -> arrow::error::Result<impl Iterator<Item = T> + 'a>
where
    T: ArrowDeserialize + ArrowField<Type = T> + 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
{
    arrow_array_deserialize_iterator_as_type::<T, T>(arr)
}

impl<Collection, Element, ArrowArray> TryIntoCollection<Collection, Element> for ArrowArray
where
    Element: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <Element as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
    ArrowArray: std::borrow::Borrow<dyn Array>,
    Collection: FromIterator<Element>,
{
    fn try_into_collection(self) -> arrow::error::Result<Collection> {
        Ok(arrow_array_deserialize_iterator::<Element>(self.borrow())?.collect())
    }

    fn try_into_collection_as_type<ArrowType>(self) -> arrow::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
        for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoArrowArrayIterator,
    {
        Ok(
            arrow_array_deserialize_iterator_as_type::<Element, ArrowType>(self.borrow())?
                .collect(),
        )
    }
}
