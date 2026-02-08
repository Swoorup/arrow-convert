//! Tests for `chrono::DateTime<Utc>` support.
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::*;
use arrow::error::Result;
use arrow_convert::deserialize::TryIntoCollection;
use arrow_convert::field::{ArrowField, DEFAULT_FIELD_NAME};
use arrow_convert::serialize::TryIntoArrow;
use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use pretty_assertions::assert_eq;

/// Basic roundtrip test for `DateTime<Utc>`
#[test]
fn test_datetime_utc_roundtrip() {
    let original_array = vec![
        Utc.timestamp_opt(1000, 0).unwrap(),
        Utc.timestamp_opt(10000, 123_456_789).unwrap(),
        Utc.timestamp_opt(-1000, 0).unwrap(),
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<DateTime<Utc>> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test roundtrip for `Option<DateTime<Utc>>`
#[test]
fn test_datetime_utc_optional_roundtrip() {
    let original_array = vec![
        Some(Utc.timestamp_opt(1000, 0).unwrap()),
        None,
        Some(Utc.timestamp_opt(10000, 123_456_789).unwrap()),
        None,
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Option<DateTime<Utc>>> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test roundtrip for `Vec<DateTime<Utc>>`
#[test]
fn test_datetime_utc_vec_roundtrip() {
    let original_array = vec![
        vec![Utc.timestamp_opt(1000, 0).unwrap(), Utc.timestamp_opt(2000, 0).unwrap()],
        vec![Utc.timestamp_opt(3000, 0).unwrap()],
        vec![],
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Vec<DateTime<Utc>>> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test that `DateTime<Utc>` has the correct Arrow schema (with timezone)
#[test]
fn test_datetime_utc_schema() {
    assert_eq!(
        <DateTime<Utc> as ArrowField>::data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );

    assert!(!<DateTime<Utc> as ArrowField>::is_nullable());
    assert!(<Option<DateTime<Utc>> as ArrowField>::is_nullable());
}

/// Test that `Vec<DateTime<Utc>>` has the correct Arrow schema
#[test]
fn test_datetime_utc_vec_schema() {
    assert_eq!(
        <Vec<DateTime<Utc>> as ArrowField>::data_type(),
        DataType::List(Arc::new(Field::new(
            DEFAULT_FIELD_NAME,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false
        )))
    );
}

/// Test that `NaiveDateTime` schema differs from `DateTime<Utc>` (timezone = None vs Some("UTC"))
#[test]
fn test_naive_vs_utc_schema_differs() {
    let naive_dt = <NaiveDateTime as ArrowField>::data_type();
    let utc_dt = <DateTime<Utc> as ArrowField>::data_type();

    assert_ne!(naive_dt, utc_dt);

    // Verify the specific difference
    assert_eq!(naive_dt, DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert_eq!(
        utc_dt,
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
}

fn data_mismatch_error<Expected: ArrowField, Actual: ArrowField>() -> arrow::error::ArrowError {
    arrow::error::ArrowError::InvalidArgumentError(format!(
        "Data type mismatch. Expected type={:#?} is_nullable={}, but was type={:#?} is_nullable={}",
        Expected::data_type(),
        Expected::is_nullable(),
        Actual::data_type(),
        Actual::is_nullable()
    ))
}

/// Test that deserializing `DateTime<Utc>` data as `NaiveDateTime` fails with a type mismatch
#[test]
fn test_datetime_utc_to_naive_mismatch_error() {
    let original_array = vec![Utc.timestamp_opt(1000, 0).unwrap(), Utc.timestamp_opt(2000, 0).unwrap()];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();

    // Attempting to deserialize as NaiveDateTime should fail
    let result: Result<Vec<NaiveDateTime>> = arrow_array.try_into_collection();

    assert_eq!(
        result.unwrap_err().to_string(),
        data_mismatch_error::<NaiveDateTime, DateTime<Utc>>().to_string()
    );
}

/// Test that deserializing `NaiveDateTime` data as `DateTime<Utc>` fails with a type mismatch
#[test]
fn test_naive_to_datetime_utc_mismatch_error() {
    let original_array = vec![
        DateTime::from_timestamp(1000, 0).unwrap().naive_utc(),
        DateTime::from_timestamp(2000, 0).unwrap().naive_utc(),
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();

    // Attempting to deserialize as DateTime<Utc> should fail
    let result: Result<Vec<DateTime<Utc>>> = arrow_array.try_into_collection();

    assert_eq!(
        result.unwrap_err().to_string(),
        data_mismatch_error::<DateTime<Utc>, NaiveDateTime>().to_string()
    );
}

/// Test roundtrip in a struct using derive macros
#[test]
fn test_datetime_utc_in_struct() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct Event {
        id: i64,
        timestamp: DateTime<Utc>,
        description: String,
    }

    let original_array = vec![
        Event {
            id: 1,
            timestamp: Utc.timestamp_opt(1000, 0).unwrap(),
            description: "first".to_string(),
        },
        Event {
            id: 2,
            timestamp: Utc.timestamp_opt(2000, 500_000_000).unwrap(),
            description: "second".to_string(),
        },
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Event> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test roundtrip with optional `DateTime<Utc>` in a struct
#[test]
fn test_datetime_utc_optional_in_struct() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct MaybeEvent {
        id: i64,
        timestamp: Option<DateTime<Utc>>,
    }

    let original_array = vec![
        MaybeEvent {
            id: 1,
            timestamp: Some(Utc.timestamp_opt(1000, 0).unwrap()),
        },
        MaybeEvent { id: 2, timestamp: None },
        MaybeEvent {
            id: 3,
            timestamp: Some(Utc.timestamp_opt(3000, 0).unwrap()),
        },
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<MaybeEvent> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test roundtrip with `Vec<DateTime<Utc>>` in a struct
#[test]
fn test_datetime_utc_vec_in_struct() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct EventLog {
        name: String,
        timestamps: Vec<DateTime<Utc>>,
    }

    let original_array = vec![
        EventLog {
            name: "log1".to_string(),
            timestamps: vec![Utc.timestamp_opt(1000, 0).unwrap(), Utc.timestamp_opt(2000, 0).unwrap()],
        },
        EventLog {
            name: "log2".to_string(),
            timestamps: vec![],
        },
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<EventLog> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);
}

/// Test struct schema includes correct timezone for `DateTime<Utc>` fields
#[test]
fn test_datetime_utc_struct_schema() {
    #[derive(Debug, ArrowField)]
    #[allow(dead_code)]
    struct Event {
        naive_ts: NaiveDateTime,
        utc_ts: DateTime<Utc>,
        optional_utc_ts: Option<DateTime<Utc>>,
        utc_ts_list: Vec<DateTime<Utc>>,
    }

    assert_eq!(
        <Event as ArrowField>::data_type(),
        DataType::Struct(Fields::from(vec![
            Field::new(
                "naive_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false
            ),
            Field::new(
                "utc_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false
            ),
            Field::new(
                "optional_utc_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true
            ),
            Field::new(
                "utc_ts_list",
                DataType::List(Arc::new(Field::new(
                    DEFAULT_FIELD_NAME,
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    false
                ))),
                false
            ),
        ]))
    );
}

/// Test that struct with mismatched datetime types fails deserialization
#[test]
fn test_struct_datetime_mismatch() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct WithUtc {
        ts: DateTime<Utc>,
    }

    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct WithNaive {
        ts: NaiveDateTime,
    }

    let original = vec![WithUtc {
        ts: Utc.timestamp_opt(1000, 0).unwrap(),
    }];

    let arrow_array: ArrayRef = original.try_into_arrow().unwrap();

    // Attempting to deserialize as WithNaive should fail
    let result: Result<Vec<WithNaive>> = arrow_array.try_into_collection();

    assert_eq!(
        result.unwrap_err().to_string(),
        data_mismatch_error::<WithNaive, WithUtc>().to_string()
    );
}

/// Test nanosecond precision is preserved
#[test]
fn test_datetime_utc_nanosecond_precision() {
    let original_array = vec![
        Utc.timestamp_nanos(1_000_000_000_123_456_789),
        Utc.timestamp_nanos(1_000_000_000_000_000_001),
        Utc.timestamp_nanos(1_000_000_000_999_999_999),
    ];

    let arrow_array: ArrayRef = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<DateTime<Utc>> = arrow_array.try_into_collection().unwrap();

    assert_eq!(original_array, round_trip);

    // Verify nanoseconds are preserved
    assert_eq!(original_array[0].timestamp_subsec_nanos(), 123_456_789);
    assert_eq!(round_trip[0].timestamp_subsec_nanos(), 123_456_789);
}
