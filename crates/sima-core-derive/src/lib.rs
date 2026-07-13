//! Derive macros for `sima-core`'s `Codec` and `TomlConfig` traits.
//!
//! Both derives are field-driven and carry no domain vocabulary: they read the
//! struct's declared fields and generate the canonical byte codec and the TOML
//! parser from the field types alone. The accepted field types are a closed set
//! — `f32`, `u32`, and `[f32; N]` — enough for every current config struct.
//!
//! `#[derive(Codec)]` emits `sima_core::Codec`; `#[derive(TomlConfig)]` emits
//! `sima_core::TomlConfig`. The generated paths are absolute (`sima_core::…`),
//! so any crate that depends on `sima-core` can apply the derives with a plain
//! `use sima_core::{Codec, TomlConfig};`.
//!
//! The generated code names its parameters at the macro's own span with a `__`
//! prefix, so a struct field may carry any name in the accepted set — including
//! `id`, `table`, or `dec` — without aliasing a generated binding.

use proc_macro::TokenStream;
use proc_macro2::{Literal, Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{Data, DeriveInput, Expr, Fields, Ident, Lit, Type, parse_macro_input};

/// The field types the derives accept, a closed set.
enum FieldKind {
    /// A single `f32`, encoded via `Enc::f32`, read as one number key.
    F32,
    /// A single `u32`, encoded via `Enc::u32`, read as one range-checked integer
    /// key.
    U32,
    /// An `[f32; N]` array, encoded as `N` contiguous `f32`s. `TomlConfig`
    /// accepts only `N == 2` (a `[lo, hi]` range).
    F32Array(usize),
}

/// A struct field the derive generates code for.
struct FieldSpec {
    ident: Ident,
    kind: FieldKind,
    /// The TOML key: the field name, or a `#[toml(key = "…")]` override.
    key: String,
}

/// `#[derive(Codec)]`: the canonical byte codec over the fields in declaration
/// order, each via the matching `Enc`/`Dec` method. `#[codec(validate = new)]`
/// routes `decode` through the type's validating constructor.
#[proc_macro_derive(Codec, attributes(codec))]
pub fn derive_codec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_codec(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[derive(TomlConfig)]`: a `parse(table, id, section)` reading each field from
/// its same-named TOML key, coercing by field type, rejecting unknown keys, and
/// routing through the constructor named by `#[toml(validate = new)]`.
#[proc_macro_derive(TomlConfig, attributes(toml))]
pub fn derive_toml_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_toml_config(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Classifies a field type against the accepted closed set, or `None` when the
/// type is outside it.
fn classify(ty: &Type) -> Option<FieldKind> {
    match ty {
        Type::Path(path) if path.qself.is_none() => {
            let ident = path.path.get_ident()?;
            if ident == "f32" {
                Some(FieldKind::F32)
            } else if ident == "u32" {
                Some(FieldKind::U32)
            } else {
                None
            }
        }
        Type::Array(array) => {
            let Type::Path(elem) = array.elem.as_ref() else {
                return None;
            };
            if elem.path.get_ident()? != "f32" {
                return None;
            }
            match &array.len {
                Expr::Lit(lit) => match &lit.lit {
                    Lit::Int(n) => Some(FieldKind::F32Array(n.base10_parse().ok()?)),
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

/// The named fields of a struct, each classified, with its TOML key resolved
/// from an optional `#[toml(key = "…")]` override. Errors on a generic struct,
/// a non-struct, tuple/unit fields, or a field type outside the accepted set.
fn field_specs(input: &DeriveInput) -> syn::Result<Vec<FieldSpec>> {
    // A generic or lifetime parameter would leave `impl … for #name` malformed;
    // reject it with the crate's own spanned error rather than a raw one.
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "this derive does not support generic or lifetime parameters",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "this derive supports structs with named fields only",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "this derive supports structs with named fields only",
        ));
    };
    named
        .named
        .iter()
        .map(|field| {
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new_spanned(field, "a named field is required"))?;
            let kind = classify(&field.ty).ok_or_else(|| {
                syn::Error::new_spanned(
                    &field.ty,
                    "unsupported field type: expected f32, u32, or [f32; N]",
                )
            })?;
            let key = toml_key_override(field)?.unwrap_or_else(|| ident.to_string());
            Ok(FieldSpec { ident, kind, key })
        })
        .collect()
}

/// The `validate = <ident>` value of a `#[codec(...)]` or `#[toml(...)]`
/// struct-level attribute named `namespace`, if present.
fn validate_target(input: &DeriveInput, namespace: &str) -> syn::Result<Option<Ident>> {
    let mut target = None;
    for attr in &input.attrs {
        if !attr.path().is_ident(namespace) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("validate") {
                target = Some(meta.value()?.parse::<Ident>()?);
                Ok(())
            } else {
                Err(meta.error(format!("unknown {namespace} attribute")))
            }
        })?;
    }
    Ok(target)
}

/// The `key = "…"` value of a field's `#[toml(...)]` attribute, if present.
fn toml_key_override(field: &syn::Field) -> syn::Result<Option<String>> {
    let mut key = None;
    for attr in &field.attrs {
        if !attr.path().is_ident("toml") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                let value: syn::LitStr = meta.value()?.parse()?;
                key = Some(value.value());
                Ok(())
            } else {
                Err(meta.error("unknown toml attribute"))
            }
        })?;
    }
    Ok(key)
}

/// Builds the constructor expression: the validating constructor when a
/// `validate` target is set, otherwise a direct struct literal wrapped in `Ok`.
fn construct(name: &Ident, validate: &Option<Ident>, idents: &[&Ident]) -> TokenStream2 {
    match validate {
        Some(func) => quote! { #name::#func( #(#idents),* ) },
        None => quote! { ::core::result::Result::Ok(#name { #(#idents),* }) },
    }
}

fn expand_codec(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let fields = field_specs(&input)?;
    let validate = validate_target(&input, "codec")?;
    let idents: Vec<&Ident> = fields.iter().map(|f| &f.ident).collect();
    // The generated parameters take `__`-prefixed names at the macro's own span,
    // so a struct field named `enc` or `dec` cannot alias them. The per-field
    // `let` bindings keep the field names and only shadow one another harmlessly.
    let enc = Ident::new("__enc", Span::mixed_site());
    let dec = Ident::new("__dec", Span::mixed_site());

    let mut encode_stmts = Vec::new();
    let mut decode_reads = Vec::new();
    for field in &fields {
        let ident = &field.ident;
        match &field.kind {
            FieldKind::F32 => {
                encode_stmts.push(quote! { #enc.f32(self.#ident); });
                decode_reads.push(quote! { let #ident = #dec.f32()?; });
            }
            FieldKind::U32 => {
                encode_stmts.push(quote! { #enc.u32(self.#ident); });
                decode_reads.push(quote! { let #ident = #dec.u32()?; });
            }
            FieldKind::F32Array(n) => {
                let mut writes = TokenStream2::new();
                for i in 0..*n {
                    let index = Literal::usize_unsuffixed(i);
                    writes.extend(quote! { #enc.f32(self.#ident[#index]); });
                }
                encode_stmts.push(writes);
                let reads = (0..*n).map(|_| quote! { #dec.f32()? });
                decode_reads.push(quote! { let #ident = [ #(#reads),* ]; });
            }
        }
    }
    let build = construct(name, &validate, &idents);
    Ok(quote! {
        impl sima_core::Codec for #name {
            fn encode(&self, #enc: &mut sima_core::Enc) {
                #(#encode_stmts)*
            }
            fn decode(#dec: &mut sima_core::Dec<'_>) -> sima_core::Result<Self> {
                #(#decode_reads)*
                #build
            }
        }
    })
}

fn expand_toml_config(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let fields = field_specs(&input)?;
    let validate = validate_target(&input, "toml")?;
    let idents: Vec<&Ident> = fields.iter().map(|f| &f.ident).collect();
    let keys: Vec<&String> = fields.iter().map(|f| &f.key).collect();

    // Two fields resolving to the same TOML key (after a `#[toml(key = "…")]`
    // override) would read one key twice and silently drop the other; reject it.
    let mut seen = std::collections::HashSet::new();
    for field in &fields {
        if !seen.insert(field.key.as_str()) {
            return Err(syn::Error::new_spanned(
                &field.ident,
                format!("two fields resolve to the same TOML key {:?}", field.key),
            ));
        }
    }

    // Parameter names at the macro's own span, so a field named `table`, `id`,
    // or `section` cannot alias them (a `u32` field `id` would otherwise be
    // passed where the helpers expect the `&str` parameter).
    let table = Ident::new("__table", Span::mixed_site());
    let id = Ident::new("__id", Span::mixed_site());
    let section = Ident::new("__section", Span::mixed_site());

    let mut reads = Vec::new();
    for field in &fields {
        let ident = &field.ident;
        let key = &field.key;
        let read = match &field.kind {
            FieldKind::F32 => {
                quote! { let #ident = sima_core::toml_config::float(#table, #id, #section, #key)?; }
            }
            FieldKind::U32 => {
                quote! { let #ident = sima_core::toml_config::integer(#table, #id, #section, #key)?; }
            }
            FieldKind::F32Array(2) => {
                quote! { let #ident = sima_core::toml_config::range(#table, #id, #section, #key)?; }
            }
            FieldKind::F32Array(_) => {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "TomlConfig accepts only [f32; 2] arrays (a [lo, hi] range)",
                ));
            }
        };
        reads.push(read);
    }
    let build = construct(name, &validate, &idents);
    Ok(quote! {
        impl sima_core::TomlConfig for #name {
            fn parse(
                #table: &toml::Table,
                #id: &str,
                #section: &str,
            ) -> sima_core::Result<Self> {
                sima_core::toml_config::reject_unknown_keys(#id, #table, &[ #(#keys),* ], #section)?;
                #(#reads)*
                #build
            }
        }
    })
}
