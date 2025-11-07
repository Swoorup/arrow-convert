#[cfg(feature = "uuid")]
mod uuid {
    use arrow::array::{Array, ArrayRef};
    use arrow::record_batch::RecordBatch;
    use arrow_convert::deserialize::*;
    use arrow_convert::field::ArrowField;
    use arrow_convert::serialize::*;
    use uuid::Uuid;

    #[test]
    fn test_uuid_serialize() {
        // Test with a known UUID
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let uuids = [uuid1, uuid2];

        // Serialize to ArrayRef
        let array: ArrayRef = uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.data_type(), &<Uuid as ArrowField>::data_type());
        assert_eq!(
            array.data_type(),
            &arrow::datatypes::DataType::FixedSizeBinary(16)
        );

        // Serialize to RecordBatch
        let rb: RecordBatch = uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.columns()[0].data_type(), &<Uuid as ArrowField>::data_type());
    }

    #[test]
    fn test_uuid_serialize_with_nulls() {
        // Test with optional UUIDs
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuids = [Some(uuid1), None, Some(Uuid::nil())];

        let array: ArrayRef = uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &<Option<Uuid> as ArrowField>::data_type());
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_uuid_deserialize() {
        // Test round-trip: serialize then deserialize
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let original_uuids = [uuid1, uuid2];

        // Serialize to Arrow array
        let array: ArrayRef = original_uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();

        // Deserialize back to Vec<Uuid>
        let deserialized: Vec<Uuid> = array.try_into_collection().unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0], uuid1);
        assert_eq!(deserialized[1], uuid2);
    }

    #[test]
    fn test_uuid_deserialize_with_nulls() {
        // Test optional UUIDs with nulls
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid_nil = Uuid::nil();
        let original_uuids = [Some(uuid1), None, Some(uuid_nil)];

        // Serialize to Arrow array
        let array: ArrayRef = original_uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();

        // Deserialize back to Vec<Option<Uuid>>
        let deserialized: Vec<Option<Uuid>> = array.try_into_collection().unwrap();

        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized[0], Some(uuid1));
        assert_eq!(deserialized[1], None);
        assert_eq!(deserialized[2], Some(uuid_nil));
    }

    #[test]
    fn test_uuid_deserialize_iterator() {
        // Test iterator-based deserialization
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let uuid3 = Uuid::nil();
        let original_uuids = [uuid1, uuid2, uuid3];

        // Serialize to Arrow array
        let array: ArrayRef = original_uuids.iter().collect::<Vec<_>>().try_into_arrow().unwrap();

        // Deserialize using iterator
        let iter = arrow_array_deserialize_iterator::<Uuid>(array.as_ref()).unwrap();

        for (deserialized, original) in iter.zip(original_uuids.iter()) {
            assert_eq!(&deserialized, original);
        }
    }

    #[test]
    fn test_uuid_vec_round_trip() {
        // Test Vec<Uuid> round-trip
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let uuid3 = Uuid::nil();
        let original = vec![vec![uuid1, uuid2, uuid3]];

        let array: ArrayRef = original.try_into_arrow().unwrap();
        let deserialized: Vec<Vec<Uuid>> = array.try_into_collection().unwrap();

        assert_eq!(deserialized, original);
    }

    #[test]
    fn test_uuid_vec_deep_nested_round_trip() {
        // Test Vec<Option<Uuid>> round-trip
        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let uuid3 = Uuid::nil();
        let original: Vec<Option<Vec<Option<Uuid>>>> = vec![Some(vec![Some(uuid1), None, Some(uuid2), Some(uuid3)])];

        let array: ArrayRef = original.try_into_arrow().unwrap();
        let deserialized: Vec<Option<Vec<Option<Uuid>>>> = array.try_into_collection().unwrap();

        assert_eq!(deserialized, original);
    }
}
