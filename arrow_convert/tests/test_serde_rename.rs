use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_convert::deserialize::TryIntoCollection;
use arrow_convert::serialize::TryIntoArrow;
use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};

#[test]
fn test_serde_rename_field() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct TestStruct {
        #[serde(rename = "renamedField")]
        my_field: i32,
        normal_field: String,
    }

    let data = vec![TestStruct {
        my_field: 42,
        normal_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"renamedField"));
    assert!(names.contains(&"normal_field"));
    assert!(!names.contains(&"my_field"));
}

#[test]
fn test_serde_rename_all_camel_case() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"myFieldName"));
    assert!(names.contains(&"anotherField"));
}

#[test]
fn test_serde_rename_all_snake_case() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "snake_case")]
    struct TestStruct {
        MyFieldName: i32,
        AnotherField: String,
    }

    let data = vec![TestStruct {
        MyFieldName: 42,
        AnotherField: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"my_field_name"));
    assert!(names.contains(&"another_field"));
}

#[test]
fn test_serde_rename_all_screaming_snake_case() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"MY_FIELD_NAME"));
    assert!(names.contains(&"ANOTHER_FIELD"));
}

#[test]
fn test_serde_rename_all_pascal_case() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "PascalCase")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"MyFieldName"));
    assert!(names.contains(&"AnotherField"));
}

#[test]
fn test_serde_rename_all_kebab_case() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "kebab-case")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"my-field-name"));
    assert!(names.contains(&"another-field"));
}

#[test]
fn test_arrow_field_precedence_over_serde() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct TestStruct {
        #[arrow_field(name = "arrow_wins")]
        #[serde(rename = "serde_loses")]
        my_field: i32,
    }

    let data = vec![TestStruct { my_field: 42 }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"arrow_wins"));
    assert!(!names.contains(&"serde_loses"));
    assert!(!names.contains(&"my_field"));
}

#[test]
fn test_serde_rename_precedence_over_rename_all() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestStruct {
        #[serde(rename = "explicit_name")]
        my_field_name: i32,
        auto_renamed: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        auto_renamed: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"explicit_name"));
    assert!(names.contains(&"autoRenamed"));
}

#[test]
fn test_serde_rename_enum_variant() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(type = "dense")]
    enum TestEnum {
        #[serde(rename = "FirstVariant")]
        VAL1,
        VAL2(i32),
    }

    let data = vec![TestEnum::VAL1, TestEnum::VAL2(42)];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    if let DataType::Union(fields, _) = arr.data_type() {
        let field_names: Vec<_> = fields.iter().map(|(_, f)| f.name().as_str()).collect();
        assert!(field_names.contains(&"FirstVariant"));
        assert!(field_names.contains(&"VAL2"));
    } else {
        panic!("Expected Union type");
    }
}

#[test]
fn test_serde_rename_all_enum() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(type = "sparse")]
    #[serde(rename_all = "snake_case")]
    enum TestEnum {
        FirstValue,
        SecondValue(i32),
    }

    let data = vec![TestEnum::FirstValue, TestEnum::SecondValue(42)];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    if let DataType::Union(fields, _) = arr.data_type() {
        let field_names: Vec<_> = fields.iter().map(|(_, f)| f.name().as_str()).collect();
        assert!(field_names.contains(&"first_value"));
        assert!(field_names.contains(&"second_value"));
    } else {
        panic!("Expected Union type");
    }
}

#[test]
fn test_round_trip_with_renames() {
    #[derive(Debug, PartialEq, Clone, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestStruct {
        user_name: String,
        user_age: i32,
        #[serde(rename = "ID")]
        user_id: i64,
    }

    let original = vec![
        TestStruct {
            user_name: "Alice".into(),
            user_age: 30,
            user_id: 1,
        },
        TestStruct {
            user_name: "Bob".into(),
            user_age: 25,
            user_id: 2,
        },
    ];

    let arr: ArrayRef = original.clone().try_into_arrow().unwrap();
    let round_trip: Vec<TestStruct> = arr.try_into_collection().unwrap();

    assert_eq!(original, round_trip);
}

#[test]
fn test_no_serde_attributes() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct TestStruct {
        my_field: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"my_field"));
    assert!(names.contains(&"another_field"));
}

#[test]
fn test_enum_round_trip_with_renames() {
    #[derive(Debug, PartialEq, Clone, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(type = "dense")]
    #[serde(rename_all = "camelCase")]
    enum TestEnum {
        FirstVariant,
        SecondVariant(i32),
    }

    let original = vec![
        TestEnum::FirstVariant,
        TestEnum::SecondVariant(42),
        TestEnum::FirstVariant,
    ];

    let arr: ArrayRef = original.clone().try_into_arrow().unwrap();
    let round_trip: Vec<TestEnum> = arr.try_into_collection().unwrap();

    assert_eq!(original, round_trip);
}

#[test]
fn test_arrow_field_rename_all() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(rename_all = "camelCase")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"myFieldName"));
    assert!(names.contains(&"anotherField"));
}

#[test]
fn test_arrow_field_rename_all_precedence_over_serde() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(rename_all = "SCREAMING_SNAKE_CASE")]
    #[serde(rename_all = "camelCase")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    // arrow_field(rename_all) should win over serde(rename_all)
    assert!(names.contains(&"MY_FIELD_NAME"));
    assert!(names.contains(&"ANOTHER_FIELD"));
}

#[test]
fn test_arrow_field_rename_all_enum() {
    #[derive(Debug, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[arrow_field(type = "dense", rename_all = "kebab-case")]
    enum TestEnum {
        FirstVariant,
        SecondVariant(i32),
    }

    let data = vec![TestEnum::FirstVariant, TestEnum::SecondVariant(42)];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    if let DataType::Union(fields, _) = arr.data_type() {
        let field_names: Vec<_> = fields.iter().map(|(_, f)| f.name().as_str()).collect();
        assert!(field_names.contains(&"first-variant"));
        assert!(field_names.contains(&"second-variant"));
    } else {
        panic!("Expected Union type");
    }
}

#[test]
fn test_compound_serde_attributes() {
    // Test that rename works when combined with other serde attributes
    #[derive(Debug, PartialEq, Clone, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct TestStruct {
        #[serde(default, rename = "renamedField")]
        my_field: i32,
        #[serde(skip_serializing_if = "Option::is_none", rename = "optionalField")]
        optional: Option<String>,
        // rename_all at field level isn't a thing, but other compound attrs should work
        normal_field: String,
    }

    let data = vec![TestStruct {
        my_field: 42,
        optional: Some("test".into()),
        normal_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"renamedField"));
    assert!(names.contains(&"optionalField"));
    assert!(names.contains(&"normal_field"));
    assert!(!names.contains(&"my_field"));
    assert!(!names.contains(&"optional"));
}

#[test]
fn test_compound_serde_with_rename_all() {
    // Test rename_all combined with other container-level serde attributes
    #[derive(Debug, PartialEq, Clone, ArrowField, ArrowSerialize, ArrowDeserialize)]
    #[serde(deny_unknown_fields, rename_all = "camelCase")]
    struct TestStruct {
        my_field_name: i32,
        another_field: String,
    }

    let data = vec![TestStruct {
        my_field_name: 42,
        another_field: "hello".into(),
    }];
    let arr: ArrayRef = data.try_into_arrow().unwrap();

    let struct_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let names = struct_arr.column_names();

    assert!(names.contains(&"myFieldName"));
    assert!(names.contains(&"anotherField"));
}
