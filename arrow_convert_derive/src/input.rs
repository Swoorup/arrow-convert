use proc_macro2::Span;
use proc_macro_error2::abort;
use quote::format_ident;

use syn::spanned::Spanned;
use syn::{DeriveInput, Ident, Lit, Visibility};

use crate::case::RenameRule;

pub const ARROW_FIELD: &str = "arrow_field";
pub const FIELD_TYPE: &str = "type";
pub const FIELD_NAME: &str = "name";
pub const FIELD_LIST_ELEMENT_NAME: &str = "list_element_name";
pub const FIELD_METADATA: &str = "metadata";
pub const FIELD_LIST_ELEMENT_METADATA: &str = "list_element_metadata";
pub const FIELD_SKIP: &str = "skip";
pub const FIELD_RENAME_ALL: &str = "rename_all";
pub const UNION_TYPE: &str = "type";
pub const UNION_TYPE_SPARSE: &str = "sparse";
pub const UNION_TYPE_DENSE: &str = "dense";
pub const TRANSPARENT: &str = "transparent";

pub struct DeriveCommon {
    /// The input name
    pub name: Ident,
    /// The overall visibility
    pub visibility: Visibility,
}

pub struct DeriveStruct {
    pub common: DeriveCommon,
    /// The list of fields in the struct
    pub fields: Vec<DeriveField>,
    pub is_transparent: bool,
    /// Container-level rename_all rule
    pub rename_all: Option<RenameRule>,
    /// Container-level list element field name override.
    pub list_element_name: Option<String>,
    /// Container-level list element metadata default.
    pub list_element_metadata: Vec<(String, String)>,
}

pub struct DeriveEnum {
    pub common: DeriveCommon,
    /// The list of variants in the enum
    pub variants: Vec<DeriveVariant>,
    pub is_dense: bool,
    /// Container-level rename_all rule
    pub rename_all: Option<RenameRule>,
}

/// All container attributes
pub struct ContainerAttrs {
    pub is_dense: Option<bool>,
    pub transparent: Option<Span>,
    /// Container-level rename_all rule
    pub rename_all: Option<RenameRule>,
    /// Container-level list element field name override.
    pub list_element_name: Option<String>,
    /// Container-level list element metadata default.
    pub list_element_metadata: Vec<(String, String)>,
}

/// All field attributes
pub struct FieldAttrs {
    pub field_type: Option<syn::Type>,
    pub field_name: Option<String>,
    pub list_element_name: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub list_element_metadata: Vec<(String, String)>,
    pub skip: bool,
}

pub struct DeriveField {
    pub syn: syn::Field,
    pub field_type: syn::Type,
    pub field_name: Option<String>,
    pub list_element_name: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub list_element_metadata: Vec<(String, String)>,
    pub skip: bool,
}

pub struct DeriveVariant {
    pub syn: syn::Variant,
    pub field_type: syn::Type,
    pub is_unit: bool,
}

impl DeriveCommon {
    pub fn from_ast(input: &DeriveInput, _container_attrs: &ContainerAttrs) -> DeriveCommon {
        DeriveCommon {
            name: input.ident.clone(),
            visibility: input.vis.clone(),
        }
    }

    pub fn mutable_array_name(&self) -> Ident {
        Ident::new(&format!("Mutable{}Array", self.name), Span::call_site())
    }

    pub fn array_name(&self) -> Ident {
        Ident::new(&format!("{}Array", self.name), Span::call_site())
    }

    pub fn iterator_name(&self) -> Ident {
        Ident::new(&format!("{}ArrayIterator", self.name), Span::call_site())
    }
}

impl ContainerAttrs {
    pub fn from_ast(attrs: &[syn::Attribute]) -> ContainerAttrs {
        let mut is_dense: Option<bool> = None;
        let mut is_transparent: Option<Span> = None;
        let mut rename_all: Option<RenameRule> = None;
        let mut list_element_name: Option<String> = None;
        let mut list_element_metadata: Vec<(String, String)> = Vec::new();

        for attr in attrs {
            if attr.path().is_ident(ARROW_FIELD) {
                if let Err(err) = attr.parse_nested_meta(|nested| {
                    if nested.path.is_ident(TRANSPARENT) {
                        is_transparent = Some(nested.path.span());
                        Ok(())
                    } else if nested.path.is_ident(UNION_TYPE) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected value for mode"));
                        };

                        match string.value().as_ref() {
                            UNION_TYPE_DENSE => {
                                is_dense = Some(true);
                                Ok(())
                            }
                            UNION_TYPE_SPARSE => {
                                is_dense = Some(false);
                                Ok(())
                            }
                            _ => Err(nested.error("Unexpected value for mode")),
                        }
                    } else if nested.path.is_ident(FIELD_RENAME_ALL) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected value for rename_all"));
                        };
                        if let Some(rule) = RenameRule::from_str(&string.value()) {
                            rename_all = Some(rule);
                        }
                        Ok(())
                    } else if nested.path.is_ident(FIELD_LIST_ELEMENT_NAME) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected value for list_element_name"));
                        };
                        list_element_name = Some(string.value());
                        Ok(())
                    } else if nested.path.is_ident(FIELD_LIST_ELEMENT_METADATA) {
                        nested.parse_nested_meta(|entry| {
                            let key = metadata_key(&entry.path)?;
                            let value = entry.value()?;
                            let Lit::Str(string) = value.parse()? else {
                                return Err(entry.error("Expected string value for metadata entry"));
                            };
                            list_element_metadata.push((key, string.value()));
                            Ok(())
                        })
                    } else {
                        Err(nested.error("Unexpected attribute"))
                    }
                }) {
                    abort!(err.span(), "{}", err);
                }
            }
        }

        ContainerAttrs {
            is_dense,
            transparent: is_transparent,
            rename_all,
            list_element_name,
            list_element_metadata,
        }
    }
}

impl FieldAttrs {
    pub fn from_ast(input: &[syn::Attribute]) -> FieldAttrs {
        let mut field_type: Option<syn::Type> = None;
        let mut field_name: Option<String> = None;
        let mut list_element_name: Option<String> = None;
        let mut metadata: Vec<(String, String)> = Vec::new();
        let mut list_element_metadata: Vec<(String, String)> = Vec::new();
        let mut skip = false;

        for attr in input {
            if attr.path().is_ident(ARROW_FIELD) {
                if let Err(err) = attr.parse_nested_meta(|nested| {
                    if nested.path.is_ident(FIELD_SKIP) {
                        skip = true;
                        Ok(())
                    } else if nested.path.is_ident(FIELD_TYPE) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected attribute"));
                        };
                        field_type = Some(syn::parse_str(&string.value())?);
                        Ok(())
                    } else if nested.path.is_ident(FIELD_NAME) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected attribute"));
                        };
                        field_name = Some(string.value());
                        Ok(())
                    } else if nested.path.is_ident(FIELD_LIST_ELEMENT_NAME) {
                        let value = nested.value()?;
                        let Lit::Str(string) = value.parse()? else {
                            return Err(nested.error("Unexpected attribute"));
                        };
                        list_element_name = Some(string.value());
                        Ok(())
                    } else if nested.path.is_ident(FIELD_METADATA) {
                        nested.parse_nested_meta(|entry| {
                            let key = metadata_key(&entry.path)?;
                            let value = entry.value()?;
                            let Lit::Str(string) = value.parse()? else {
                                return Err(entry.error("Expected string value for metadata entry"));
                            };
                            metadata.push((key, string.value()));
                            Ok(())
                        })
                    } else if nested.path.is_ident(FIELD_LIST_ELEMENT_METADATA) {
                        nested.parse_nested_meta(|entry| {
                            let key = metadata_key(&entry.path)?;
                            let value = entry.value()?;
                            let Lit::Str(string) = value.parse()? else {
                                return Err(entry.error("Expected string value for metadata entry"));
                            };
                            list_element_metadata.push((key, string.value()));
                            Ok(())
                        })
                    } else {
                        Err(nested.error("Unexpected attribute"))
                    }
                }) {
                    abort!(err.span(), "{}", err);
                }
            }
        }

        FieldAttrs {
            field_type,
            field_name,
            list_element_name,
            metadata,
            list_element_metadata,
            skip,
        }
    }
}

impl DeriveStruct {
    pub fn from_ast(input: &DeriveInput, ast: &syn::DataStruct) -> DeriveStruct {
        let container_attrs = ContainerAttrs::from_ast(&input.attrs);
        let common = DeriveCommon::from_ast(input, &container_attrs);

        let is_transparent = if let Some(span) = container_attrs.transparent {
            if ast.fields.len() > 1 {
                abort!(span, "'transparent' is only supported on length-1 structs!");
            }
            true
        } else {
            false
        };

        DeriveStruct {
            common,
            fields: ast.fields.iter().map(DeriveField::from_ast).collect::<Vec<_>>(),
            is_transparent,
            rename_all: container_attrs.rename_all,
            list_element_name: container_attrs.list_element_name,
            list_element_metadata: container_attrs.list_element_metadata,
        }
    }
}

impl DeriveEnum {
    pub fn from_ast(input: &DeriveInput, ast: &syn::DataEnum) -> DeriveEnum {
        let container_attrs = ContainerAttrs::from_ast(&input.attrs);
        let common = DeriveCommon::from_ast(input, &container_attrs);

        DeriveEnum {
            common,
            variants: ast.variants.iter().map(DeriveVariant::from_ast).collect::<Vec<_>>(),
            is_dense: container_attrs
                .is_dense
                .unwrap_or_else(|| abort!(input.span(), "Missing mode attribute for enum")),
            rename_all: container_attrs.rename_all,
        }
    }
}

impl DeriveField {
    pub fn from_ast(input: &syn::Field) -> DeriveField {
        let attrs = FieldAttrs::from_ast(&input.attrs);

        DeriveField {
            syn: input.clone(),
            field_type: attrs.field_type.unwrap_or_else(|| input.ty.clone()),
            field_name: attrs.field_name,
            list_element_name: attrs.list_element_name,
            metadata: attrs.metadata,
            list_element_metadata: attrs.list_element_metadata,
            skip: attrs.skip,
        }
    }

    /// Get the effective field name considering precedence:
    /// arrow_field(name) > rename_all applied to rust name > rust name
    pub fn effective_name(&self, index: usize, rename_all: Option<RenameRule>) -> String {
        // 1. arrow_field(name = "...") takes highest precedence
        if let Some(name) = &self.field_name {
            return name.clone();
        }

        // 2. Get rust field name (using format_ident to strip r# prefix from raw identifiers)
        let rust_name = match self.syn.ident.as_ref() {
            Some(ident) => format_ident!("{}", ident).to_string(),
            None => return format!("field_{}", index), // tuple struct
        };

        // 3. Apply container-level rename_all if present
        if let Some(rule) = rename_all {
            return rule.apply(&rust_name);
        }

        rust_name
    }

    /// Get the effective list element field name considering precedence:
    /// arrow_field(list_element_name) > container list_element_name
    pub fn effective_list_element_name(&self, container_list_element_name: Option<&str>) -> Option<String> {
        self.list_element_name
            .clone()
            .or_else(|| container_list_element_name.map(str::to_string))
    }

    pub fn effective_list_element_metadata(
        &self,
        container_list_element_metadata: &[(String, String)],
    ) -> Vec<(String, String)> {
        merge_metadata_entries(container_list_element_metadata, &self.list_element_metadata)
    }
}

fn merge_metadata_entries(base: &[(String, String)], overrides: &[(String, String)]) -> Vec<(String, String)> {
    let mut merged: Vec<(String, String)> = Vec::new();
    for (key, value) in base.iter().chain(overrides.iter()) {
        if let Some(existing_idx) = merged.iter().position(|(k, _)| k == key) {
            merged.remove(existing_idx);
        }
        merged.push((key.clone(), value.clone()));
    }
    merged
}

fn metadata_key(path: &syn::Path) -> syn::Result<String> {
    if path.segments.is_empty() || path.segments.iter().any(|s| !s.arguments.is_empty()) {
        return Err(syn::Error::new(
            path.span(),
            "Expected metadata key identifier path",
        ));
    }
    Ok(path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join(":"))
}

impl DeriveVariant {
    pub fn from_ast(input: &syn::Variant) -> DeriveVariant {
        let attrs = FieldAttrs::from_ast(&input.attrs);

        let (is_unit, field_type) = match &input.fields {
            syn::Fields::Named(_f) => {
                unimplemented!()
            }
            syn::Fields::Unnamed(f) => {
                if f.unnamed.len() > 1 {
                    unimplemented!()
                } else {
                    (false, f.unnamed[0].ty.clone())
                }
            }

            syn::Fields::Unit => (true, syn::parse_str("bool").unwrap()),
        };
        DeriveVariant {
            syn: input.clone(),
            field_type: attrs.field_type.unwrap_or_else(|| field_type.clone()),
            is_unit,
        }
    }

    /// Get the effective variant name considering precedence:
    /// rename_all applied to rust name > rust name
    pub fn effective_name(&self, rename_all: Option<RenameRule>) -> String {
        // Use format_ident to strip r# prefix from raw identifiers (for consistency)
        let rust_name = format_ident!("{}", self.syn.ident).to_string();

        // Apply container-level rename_all if present
        if let Some(rule) = rename_all {
            return rule.apply(&rust_name);
        }

        rust_name
    }
}
