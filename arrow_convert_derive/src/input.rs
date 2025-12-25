use proc_macro2::Span;
use proc_macro_error2::abort;
use quote::format_ident;

use syn::spanned::Spanned;
use syn::{DeriveInput, Ident, Lit, Meta, Visibility};

use crate::case::RenameRule;

pub const ARROW_FIELD: &str = "arrow_field";
pub const FIELD_TYPE: &str = "type";
pub const FIELD_NAME: &str = "name";
pub const FIELD_SKIP: &str = "skip";
pub const FIELD_RENAME_ALL: &str = "rename_all";
pub const UNION_TYPE: &str = "type";
pub const UNION_TYPE_SPARSE: &str = "sparse";
pub const UNION_TYPE_DENSE: &str = "dense";
pub const TRANSPARENT: &str = "transparent";

pub const SERDE: &str = "serde";
pub const SERDE_RENAME: &str = "rename";
pub const SERDE_RENAME_ALL: &str = "rename_all";

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
    /// Container-level rename_all rule (arrow_field takes precedence over serde)
    pub rename_all: Option<RenameRule>,
}

pub struct DeriveEnum {
    pub common: DeriveCommon,
    /// The list of variants in the enum
    pub variants: Vec<DeriveVariant>,
    pub is_dense: bool,
    /// Container-level rename_all rule (arrow_field takes precedence over serde)
    pub rename_all: Option<RenameRule>,
}

/// All container attributes
pub struct ContainerAttrs {
    pub is_dense: Option<bool>,
    pub transparent: Option<Span>,
    /// Effective rename_all rule (arrow_field takes precedence over serde)
    pub rename_all: Option<RenameRule>,
}

/// All field attributes
pub struct FieldAttrs {
    pub field_type: Option<syn::Type>,
    pub field_name: Option<String>,
    pub skip: bool,
    /// Serde rename for this field
    pub serde_rename: Option<String>,
}

pub struct DeriveField {
    pub syn: syn::Field,
    pub field_type: syn::Type,
    pub field_name: Option<String>,
    pub skip: bool,
    /// Serde rename for this field
    pub serde_rename: Option<String>,
}

pub struct DeriveVariant {
    pub syn: syn::Variant,
    pub field_type: syn::Type,
    pub is_unit: bool,
    /// Serde rename for this variant
    pub serde_rename: Option<String>,
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
        let mut arrow_rename_all: Option<RenameRule> = None;
        let mut serde_rename_all: Option<RenameRule> = None;

        for attr in attrs {
            if attr.path().is_ident(ARROW_FIELD) {
                let _ = attr.parse_nested_meta(|meta| {
                    if let Meta::List(list) = &attr.meta {
                        list.parse_nested_meta(|nested| {
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
                                    arrow_rename_all = Some(rule);
                                }
                                Ok(())
                            } else {
                                Err(meta.error("Unexpected attribute"))
                            }
                        })?;
                    };

                    Ok(())
                });
            } else if attr.path().is_ident(SERDE) {
                // Parse serde attributes - silently ignore errors/unknown attributes
                let _ = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident(SERDE_RENAME_ALL) {
                        let value = meta.value()?;
                        let lit: syn::LitStr = value.parse()?;
                        if let Some(rule) = RenameRule::from_str(&lit.value()) {
                            serde_rename_all = Some(rule);
                        }
                    } else if meta.input.peek(syn::Token![=]) {
                        // Consume value for unknown key=value attributes
                        let _: syn::Token![=] = meta.input.parse()?;
                        let _: syn::Lit = meta.input.parse()?;
                    }
                    // Path-only attributes like `default` don't need special handling
                    Ok(())
                });
            }
        }

        // arrow_field(rename_all) takes precedence over serde(rename_all)
        let rename_all = arrow_rename_all.or(serde_rename_all);

        ContainerAttrs {
            is_dense,
            transparent: is_transparent,
            rename_all,
        }
    }
}

impl FieldAttrs {
    pub fn from_ast(input: &[syn::Attribute]) -> FieldAttrs {
        let mut field_type: Option<syn::Type> = None;
        let mut field_name: Option<String> = None;
        let mut skip = false;
        let mut serde_rename: Option<String> = None;

        for attr in input {
            if attr.path().is_ident(ARROW_FIELD) {
                attr.parse_nested_meta(|meta| {
                    let Meta::List(list) = &attr.meta else {
                        return Err(meta.error("Unexpected attribute"));
                    };

                    list.parse_nested_meta(|nested| {
                        if nested.path.is_ident(FIELD_SKIP) {
                            skip = true;
                        } else if nested.path.is_ident(FIELD_TYPE) {
                            let value = nested.value()?;
                            let Lit::Str(string) = value.parse()? else {
                                return Err(meta.error("Unexpected attribute"));
                            };
                            field_type = Some(syn::parse_str(&string.value())?);
                        } else if nested.path.is_ident(FIELD_NAME) {
                            let value = nested.value()?;
                            let Lit::Str(string) = value.parse()? else {
                                return Err(meta.error("Unexpected attribute"));
                            };
                            field_name = Some(string.value());
                        } else {
                            return Err(meta.error("Unexpected attribute"));
                        }
                        Ok(())
                    })
                })
                .unwrap_or_default();
            } else if attr.path().is_ident(SERDE) {
                // Parse serde attributes - silently ignore errors/unknown attributes
                let _ = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident(SERDE_RENAME) {
                        let value = meta.value()?;
                        let lit: syn::LitStr = value.parse()?;
                        serde_rename = Some(lit.value());
                    } else if meta.input.peek(syn::Token![=]) {
                        // Consume value for unknown key=value attributes (e.g., `with = "..."`)
                        let _: syn::Token![=] = meta.input.parse()?;
                        let _: syn::Lit = meta.input.parse()?;
                    }
                    // Path-only attributes like `default` don't need special handling
                    Ok(())
                });
            }
        }

        FieldAttrs {
            field_type,
            field_name,
            skip,
            serde_rename,
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
            skip: attrs.skip,
            serde_rename: attrs.serde_rename,
        }
    }

    /// Get the effective field name considering precedence:
    /// arrow_field(name) > serde(rename) > rename_all applied to rust name > rust name
    pub fn effective_name(&self, index: usize, rename_all: Option<RenameRule>) -> String {
        // 1. arrow_field(name = "...") takes highest precedence
        if let Some(name) = &self.field_name {
            return name.clone();
        }

        // 2. serde(rename = "...") takes second precedence
        if let Some(name) = &self.serde_rename {
            return name.clone();
        }

        // 3. Get rust field name (using format_ident to strip r# prefix from raw identifiers)
        let rust_name = match self.syn.ident.as_ref() {
            Some(ident) => format_ident!("{}", ident).to_string(),
            None => return format!("field_{}", index), // tuple struct
        };

        // 4. Apply container-level rename_all if present
        if let Some(rule) = rename_all {
            return rule.apply(&rust_name);
        }

        rust_name
    }
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
            serde_rename: attrs.serde_rename,
        }
    }

    /// Get the effective variant name considering precedence:
    /// serde(rename) > rename_all applied to rust name > rust name
    pub fn effective_name(&self, rename_all: Option<RenameRule>) -> String {
        // 1. serde(rename = "...") takes precedence
        if let Some(name) = &self.serde_rename {
            return name.clone();
        }

        // Use format_ident to strip r# prefix from raw identifiers (for consistency)
        let rust_name = format_ident!("{}", self.syn.ident).to_string();

        // 2. Apply container-level rename_all if present
        if let Some(rule) = rename_all {
            return rule.apply(&rust_name);
        }

        rust_name
    }
}
