/// Case conversion rules matching serde's `rename_all` attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameRule {
    Lower,
    Upper,
    Camel,
    Pascal,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
}

impl RenameRule {
    /// Parse a serde rename_all string into a RenameRule.
    pub fn from_str(s: &str) -> Option<RenameRule> {
        match s {
            "lowercase" => Some(RenameRule::Lower),
            "UPPERCASE" => Some(RenameRule::Upper),
            "camelCase" => Some(RenameRule::Camel),
            "PascalCase" => Some(RenameRule::Pascal),
            "snake_case" => Some(RenameRule::Snake),
            "SCREAMING_SNAKE_CASE" => Some(RenameRule::ScreamingSnake),
            "kebab-case" => Some(RenameRule::Kebab),
            "SCREAMING-KEBAB-CASE" => Some(RenameRule::ScreamingKebab),
            _ => None,
        }
    }

    /// Apply the rename rule to a field/variant name.
    pub fn apply(&self, name: &str) -> String {
        let words = split_into_words(name);
        match self {
            RenameRule::Lower => words.concat().to_lowercase(),
            RenameRule::Upper => words.concat().to_uppercase(),
            RenameRule::Camel => {
                let mut result = String::new();
                for (i, word) in words.iter().enumerate() {
                    if i == 0 {
                        result.push_str(&word.to_lowercase());
                    } else {
                        result.push_str(&capitalize(word));
                    }
                }

                result
            }
            RenameRule::Pascal => words.iter().map(|w| capitalize(w)).collect(),
            RenameRule::Snake => words.iter().map(|w| w.to_lowercase()).collect::<Vec<_>>().join("_"),
            RenameRule::ScreamingSnake => words.iter().map(|w| w.to_uppercase()).collect::<Vec<_>>().join("_"),
            RenameRule::Kebab => words.iter().map(|w| w.to_lowercase()).collect::<Vec<_>>().join("-"),
            RenameRule::ScreamingKebab => words.iter().map(|w| w.to_uppercase()).collect::<Vec<_>>().join("-"),
        }
    }
}

/// Split a name into words based on underscores, hyphens, and case transitions.
fn split_into_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current_word = String::new();
    let mut chars = name.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '_' || ch == '-' {
            if !current_word.is_empty() {
                words.push(std::mem::take(&mut current_word));
            }
        } else if ch.is_uppercase() {
            if !current_word.is_empty() {
                let next_is_lower = chars.peek().is_some_and(|c| c.is_lowercase());
                let current_all_upper = current_word.chars().all(|c| c.is_uppercase());

                if current_all_upper && next_is_lower {
                    // We're at an uppercase char followed by lowercase, ending an acronym run
                    // Example: "XMLParser" at 'P' with current_word="XML", next='a'
                    // Push current_word as a complete acronym and start new word with current char
                    words.push(std::mem::take(&mut current_word));
                } else if !current_all_upper {
                    // Regular camelCase transition: lowercase->uppercase
                    // Example: "myField" at 'F' with current_word="my"
                    words.push(std::mem::take(&mut current_word));
                }
                // If current_all_upper && !next_is_lower, we're continuing an acronym run
            }
            current_word.push(ch);
        } else {
            current_word.push(ch);
        }
    }

    if !current_word.is_empty() {
        words.push(current_word);
    }

    words
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first
            .to_uppercase()
            .chain(chars.flat_map(|c| c.to_lowercase()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_snake_case() {
        assert_eq!(split_into_words("my_field_name"), vec!["my", "field", "name"]);
    }

    #[test]
    fn test_split_camel_case() {
        assert_eq!(split_into_words("myFieldName"), vec!["my", "Field", "Name"]);
    }

    #[test]
    fn test_split_pascal_case() {
        assert_eq!(split_into_words("MyFieldName"), vec!["My", "Field", "Name"]);
    }

    #[test]
    fn test_split_acronym() {
        assert_eq!(split_into_words("XMLParser"), vec!["XML", "Parser"]);
        assert_eq!(split_into_words("parseXML"), vec!["parse", "XML"]);
    }

    #[test]
    fn test_apply_camel_case() {
        assert_eq!(RenameRule::Camel.apply("my_field_name"), "myFieldName");
        assert_eq!(RenameRule::Camel.apply("MyFieldName"), "myFieldName");
    }

    #[test]
    fn test_apply_pascal_case() {
        assert_eq!(RenameRule::Pascal.apply("my_field_name"), "MyFieldName");
    }

    #[test]
    fn test_apply_snake_case() {
        assert_eq!(RenameRule::Snake.apply("myFieldName"), "my_field_name");
        assert_eq!(RenameRule::Snake.apply("MyFieldName"), "my_field_name");
    }

    #[test]
    fn test_apply_screaming_snake_case() {
        assert_eq!(RenameRule::ScreamingSnake.apply("myFieldName"), "MY_FIELD_NAME");
    }

    #[test]
    fn test_apply_kebab_case() {
        assert_eq!(RenameRule::Kebab.apply("my_field_name"), "my-field-name");
    }

    #[test]
    fn test_apply_lowercase() {
        assert_eq!(RenameRule::Lower.apply("MyFieldName"), "myfieldname");
    }

    #[test]
    fn test_apply_uppercase() {
        assert_eq!(RenameRule::Upper.apply("my_field_name"), "MYFIELDNAME");
    }
}
