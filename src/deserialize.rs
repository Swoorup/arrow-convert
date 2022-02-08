// Implementations of derive traits for arrow2 built-in types

use arrow2::array::*;
use chrono::{NaiveDate,NaiveDateTime};

use crate::*;

/// Implemented by all arrow fields that can deserialize from arrow
pub trait ArrowDeserialize: ArrowField + Sized
    where Self::ArrayType: ArrowArray,
        for<'a> &'a Self::ArrayType: IntoIterator
{
    type ArrayType;

    /// Deserialize this field from arrow
    fn arrow_deserialize<'a>(v: <&'a Self::ArrayType as IntoIterator>::Item) -> Option<Self>;

    #[inline]
    // For internal use only
    //
    // This is an ugly hack to allow generating a blanket Option<T> deserialize. 
    // Ideally we would be able to capture the optional field of the iterator via 
    // something like for<'a> &'a T::ArrayType: IntoIterator<Item=Option<E>>,
    // However, the E parameter seems to confuse the borrow checker if it's a reference.
    fn arrow_deserialize_internal<'a>(v: <&'a Self::ArrayType as IntoIterator>::Item) -> Self {
        Self::arrow_deserialize(v).unwrap()
    }
}

/// Internal trait used to support deserialization and iteration of structs, and nested struct lists
/// 
/// Trivial pass-thru implementations are provided for arrow2 arrays that implement IntoIterator.
/// 
/// The derive macro generates implementations for typed struct arrays.
#[doc(hidden)]
pub trait ArrowArray: 
    where for<'a> &'a Self: IntoIterator
{
    type BaseArrayType: Array;

    // Returns a typed iterator to the underlying elements of the array from an untyped Array reference.
    fn iter_from_array_ref<'a>(b: &'a dyn Array)  -> arrow2::error::Result<<&'a Self as IntoIterator>::IntoIter>;
}

// Macro to facilitate implementation for numeric types and numeric arrays.
macro_rules! impl_arrow_deserialize_primitive {
    ($physical_type:ty, $logical_type:ident) => {
        impl ArrowDeserialize for $physical_type {
            type ArrayType = PrimitiveArray<$physical_type>;

            #[inline]
            fn arrow_deserialize<'a>(v: Option<&$physical_type>) -> Option<Self> {
                v.map(|t| *t)
            }
        }

        impl_arrow_array!(PrimitiveArray<$physical_type>);
    };
}

macro_rules! impl_arrow_array {
    ($array:ty) => {
        impl ArrowArray for $array {
            type BaseArrayType = Self;

            fn iter_from_array_ref<'a>(b: &'a dyn Array)  -> arrow2::error::Result<<&'a Self as IntoIterator>::IntoIter> {
                Ok(b.as_any().downcast_ref::<Self::BaseArrayType>().unwrap().into_iter())
            }        
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowDeserialize for Option<T>
where T: ArrowDeserialize,
    T::ArrayType: 'static + ArrowArray,
    for<'a> &'a T::ArrayType: IntoIterator,
{
    type ArrayType = <T as ArrowDeserialize>::ArrayType;

    #[inline]
    fn arrow_deserialize<'a>(v: <&'a Self::ArrayType as IntoIterator>::Item) -> Option<Self> {
        Some(Self::arrow_deserialize_internal(v))
    }

    #[inline]
    fn arrow_deserialize_internal<'a>(v: <&'a Self::ArrayType as IntoIterator>::Item) -> Self
    {
        <T as ArrowDeserialize>::arrow_deserialize(v)
    }
}

impl_arrow_deserialize_primitive!(u8, UInt8);
impl_arrow_deserialize_primitive!(u16, UInt16);
impl_arrow_deserialize_primitive!(u32, UInt32);
impl_arrow_deserialize_primitive!(u64, UInt64);
impl_arrow_deserialize_primitive!(i8, Int8);
impl_arrow_deserialize_primitive!(i16, Int16);
impl_arrow_deserialize_primitive!(i32, Int32);
impl_arrow_deserialize_primitive!(i64, Int64);
impl_arrow_deserialize_primitive!(f32, Float32);
impl_arrow_deserialize_primitive!(f64, Float64);

impl ArrowDeserialize for String
{
    type ArrayType = Utf8Array<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&str>) -> Option<Self> {
        v.map(|t| t.to_string())
    }
}

impl ArrowDeserialize for bool
{
    type ArrayType = BooleanArray;

    #[inline]
    fn arrow_deserialize(v: Option<bool>) -> Option<Self>
    {
        v
    }
}

impl ArrowDeserialize for NaiveDateTime
{
    type ArrayType = PrimitiveArray<i64>;

    #[inline]
    fn arrow_deserialize(v: Option<&i64>) -> Option<Self> {
        v.map(|t|arrow2::temporal_conversions::timestamp_ns_to_datetime(*t))
    }
}

impl ArrowDeserialize for NaiveDate
{
    type ArrayType = PrimitiveArray<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&i32>) -> Option<Self> {
        v.map(|t| arrow2::temporal_conversions::date32_to_date(*t))
    }
}

impl<'a> ArrowDeserialize for Vec<u8> {
    type ArrayType = BinaryArray<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.map(|t|t.to_vec())
    }
}

// Blanket implementation for Vec
impl<T> ArrowDeserialize for Vec<T>
where T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator
{
    type ArrayType = ListArray<i32>;

    fn arrow_deserialize(v: Option<Box<dyn Array>>) -> Option<Self> {
        use std::ops::Deref;
        match v {
            Some(t) => {
                arrow_array_deserialize_iter(t.deref()).ok().map(|i| i.collect::<Vec<T>>())
            }
            None => None
        }
    }
}

impl_arrow_array!(BooleanArray);
impl_arrow_array!(Utf8Array<i32>);
impl_arrow_array!(BinaryArray<i32>);
impl_arrow_array!(ListArray<i32>);


/// Top-level API to deserialize from Arrow
pub trait FromArrow<T>
{
    fn from_arrow(self) -> arrow2::error::Result<T>;
}

impl<'a,T> FromArrow<Vec<T>> for &'a dyn Array 
where T: ArrowDeserialize + 'static, 
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    fn from_arrow(self) -> arrow2::error::Result<Vec<T>> {
        if &<T as ArrowField>::data_type() != self.data_type() {
            Err(arrow2::error::ArrowError::InvalidArgumentError("Data type mismatch".to_string()))
        }
        else {
            Ok(arrow_array_deserialize_iter(self)?.collect())
        }
    }
}

// Blanket implemented for Box<dyn Array>
impl<'a, T> FromArrow<T> for &'a Box<dyn Array>
where &'a dyn Array: FromArrow<T>
{
    fn from_arrow(self) -> arrow2::error::Result<T> {
        use std::ops::Deref;

        self.deref().from_arrow()
    }
}

// Blanket implemented for Arc<dyn Array>
impl<'a, T> FromArrow<T> for &'a std::sync::Arc<dyn Array>
where &'a dyn Array: FromArrow<T>
{
    fn from_arrow(self) -> arrow2::error::Result<T> {
        use std::ops::Deref;

        self.deref().from_arrow()
    }
}

/// Internal helper to return an iterator for elements from a [`arrow2::array::Array`]. 
/// Does not perform any schema checks.
fn arrow_array_deserialize_iter<'a, T>(b: &'a dyn arrow2::array::Array) -> arrow2::error::Result<impl Iterator<Item = T> + 'a>
where T: ArrowDeserialize + 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator
{    
    Ok(<<T as ArrowDeserialize>::ArrayType as ArrowArray>::iter_from_array_ref(b)?
        .map(<T as ArrowDeserialize>::arrow_deserialize_internal))
}