use arrow_convert::ArrowField;

#[derive(ArrowField)]
struct MetadataNotString {
    #[arrow_field(metadata(role = 1))]
    value: i64,
}

fn main() {}
