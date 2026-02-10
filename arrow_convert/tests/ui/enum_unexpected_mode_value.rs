use arrow_convert::ArrowField;

#[derive(ArrowField)]
#[arrow_field(type = "invalid_mode")]
enum BadMode {
    A,
}

fn main() {}
