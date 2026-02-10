use arrow_convert::ArrowField;

#[derive(ArrowField)]
#[arrow_field(rename_all = 42)]
struct RenameAllNotString {
    value: i64,
}

fn main() {}
