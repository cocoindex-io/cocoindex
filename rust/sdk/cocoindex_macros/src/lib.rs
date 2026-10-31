//! Proc macros for cocoindex: `#[function]`, `#[app]`, `#[lifespan]`, `#[main]`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use syn::{
    DeriveInput, Error as SynError, Expr, FnArg, Ident, ItemFn, LitInt, LitStr, Pat, PatType, Path,
    Stmt, Token, Type, TypeReference, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// Derive structural, recursively compositional memoization behavior.
///
/// Every field must implement `cocoindex::MemoInput`; the containing struct or
/// enum does not need to implement `serde::Serialize`. The generated identity
/// includes the type's module path and name, so moving the type to another
/// module causes a one-time cache miss. Renaming the type or a named field also
/// changes its identity. These invalidations affect cached values only and do
/// not change persisted context-key names.
#[proc_macro_derive(MemoInput)]
pub fn derive_memo_input(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_memo_input_impl(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn derive_memo_input_impl(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident;
    let mut generics = input.generics;
    for ty in memo_input_field_types(&input.data) {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: ::cocoindex::memo::MemoInput));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let (write_body, state_body) = memo_input_derive_bodies(&name, &input.data)?;

    Ok(quote! {
        #[::cocoindex::async_trait]
        impl #impl_generics ::cocoindex::memo::MemoInput for #name #ty_generics #where_clause {
            fn write_memo_key(
                &self,
                writer: &mut ::cocoindex::memo::MemoKeyWriter<'_>,
            ) -> ::cocoindex::Result<()> {
                #write_body
            }

            async fn memo_state(
                &self,
                previous: ::core::option::Option<&::cocoindex::memo::MemoStateValue>,
            ) -> ::cocoindex::Result<
                ::core::option::Option<::cocoindex::memo::MemoStateDecision>
            > {
                #state_body
            }
        }
    })
}

fn memo_input_field_types(data: &syn::Data) -> Vec<&Type> {
    match data {
        syn::Data::Struct(data) => data.fields.iter().map(|field| &field.ty).collect(),
        syn::Data::Enum(data) => data
            .variants
            .iter()
            .flat_map(|variant| variant.fields.iter().map(|field| &field.ty))
            .collect(),
        syn::Data::Union(_) => Vec::new(),
    }
}

fn memo_input_derive_bodies(
    name: &Ident,
    data: &syn::Data,
) -> syn::Result<(TokenStream2, TokenStream2)> {
    match data {
        syn::Data::Struct(data) => Ok(memo_input_struct_bodies(name, &data.fields)),
        syn::Data::Enum(data) => Ok(memo_input_enum_bodies(name, data)),
        syn::Data::Union(data) => Err(SynError::new_spanned(
            data.union_token,
            "MemoInput cannot be derived for unions",
        )),
    }
}

fn memo_input_struct_bodies(name: &Ident, fields: &syn::Fields) -> (TokenStream2, TokenStream2) {
    let len = fields.len();
    let (key_writes, children): (Vec<_>, Vec<_>) = match fields {
        syn::Fields::Named(fields) => fields
            .named
            .iter()
            .map(|field| {
                let ident = field.ident.as_ref().expect("named field has an identifier");
                let field_name = ident.to_string();
                (
                    quote! {
                        writer.write(&#field_name)?;
                        writer.write_input(&self.#ident)?;
                    },
                    quote! { &self.#ident as &dyn ::cocoindex::memo::MemoInput },
                )
            })
            .unzip(),
        syn::Fields::Unnamed(fields) => fields
            .unnamed
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let index = syn::Index::from(index);
                (
                    quote! { writer.write_input(&self.#index)?; },
                    quote! { &self.#index as &dyn ::cocoindex::memo::MemoInput },
                )
            })
            .unzip(),
        syn::Fields::Unit => (Vec::new(), Vec::new()),
    };
    let write_body = quote! {
        writer.write(&("struct", ::core::module_path!(), ::core::stringify!(#name), #len))?;
        #(#key_writes)*
        Ok(())
    };
    let state_body = quote! {
        let __coco_children: &[&dyn ::cocoindex::memo::MemoInput] = &[#(#children),*];
        ::cocoindex::memo::memo_state_group(__coco_children, previous, false).await
    };
    (write_body, state_body)
}

fn memo_input_enum_bodies(name: &Ident, data: &syn::DataEnum) -> (TokenStream2, TokenStream2) {
    let mut write_arms = Vec::with_capacity(data.variants.len());
    let mut state_arms = Vec::with_capacity(data.variants.len());

    for variant in &data.variants {
        let variant_ident = &variant.ident;
        let variant_name = variant_ident.to_string();
        let len = variant.fields.len();
        match &variant.fields {
            syn::Fields::Named(fields) => {
                let fields_and_bindings = fields
                    .named
                    .iter()
                    .map(|field| {
                        let field = field.ident.as_ref().expect("named field has an identifier");
                        let binding = format_ident!("__coco_field_{}", field);
                        (field, binding)
                    })
                    .collect::<Vec<_>>();
                let key_writes = fields_and_bindings.iter().map(|(field, binding)| {
                    let field_name = field.to_string();
                    quote! {
                        writer.write(&#field_name)?;
                        writer.write_input(#binding)?;
                    }
                });
                let children = fields_and_bindings
                    .iter()
                    .map(|(_, binding)| quote! { #binding as &dyn ::cocoindex::memo::MemoInput });
                let patterns = fields_and_bindings
                    .iter()
                    .map(|(field, binding)| quote! { #field: #binding });
                write_arms.push(quote! {
                    Self::#variant_ident { #(#patterns),* } => {
                        writer.write(&(
                            "enum",
                            ::core::module_path!(),
                            ::core::stringify!(#name),
                            #variant_name,
                            #len,
                        ))?;
                        #(#key_writes)*
                        Ok(())
                    }
                });
                let patterns = fields_and_bindings
                    .iter()
                    .map(|(field, binding)| quote! { #field: #binding });
                state_arms.push(quote! {
                    Self::#variant_ident { #(#patterns),* } => {
                        let __coco_children: &[&dyn ::cocoindex::memo::MemoInput] =
                            &[#(#children),*];
                        ::cocoindex::memo::memo_state_group(
                            __coco_children,
                            previous,
                            false,
                        ).await
                    }
                });
            }
            syn::Fields::Unnamed(fields) => {
                let bindings = (0..fields.unnamed.len())
                    .map(|index| format_ident!("__coco_field_{index}"))
                    .collect::<Vec<_>>();
                let key_writes = bindings
                    .iter()
                    .map(|binding| quote! { writer.write_input(#binding)?; });
                let children = bindings
                    .iter()
                    .map(|binding| quote! { #binding as &dyn ::cocoindex::memo::MemoInput });
                write_arms.push(quote! {
                    Self::#variant_ident(#(#bindings),*) => {
                        writer.write(&(
                            "enum",
                            ::core::module_path!(),
                            ::core::stringify!(#name),
                            #variant_name,
                            #len,
                        ))?;
                        #(#key_writes)*
                        Ok(())
                    }
                });
                state_arms.push(quote! {
                    Self::#variant_ident(#(#bindings),*) => {
                        let __coco_children: &[&dyn ::cocoindex::memo::MemoInput] =
                            &[#(#children),*];
                        ::cocoindex::memo::memo_state_group(
                            __coco_children,
                            previous,
                            false,
                        ).await
                    }
                });
            }
            syn::Fields::Unit => {
                write_arms.push(quote! {
                    Self::#variant_ident => {
                        writer.write(&(
                            "enum",
                            ::core::module_path!(),
                            ::core::stringify!(#name),
                            #variant_name,
                            0usize,
                        ))?;
                        Ok(())
                    }
                });
                state_arms.push(quote! { Self::#variant_ident => Ok(None) });
            }
        }
    }

    (
        quote! { match self { #(#write_arms),* } },
        quote! { match self { #(#state_arms),* } },
    )
}

/// Information about a non-ctx parameter.
#[derive(Clone, Debug)]
struct ParamInfo {
    ident: syn::Ident,
    ty: Type,
    is_ref: bool,
    is_str_ref: bool,
}

/// Parse an async fn and extract the optional `&Ctx` parameter name + non-ctx
/// parameter info. A `&Ctx` parameter is not required: a ctx-free function (e.g.
/// a batch implementation for `coco::Batched`) still gets a logic-hash const and
/// registration, just no dependency-tracking wrapper.
fn parse_fn_params_opt(func: &ItemFn) -> syn::Result<(Option<syn::Ident>, Vec<ParamInfo>)> {
    let mut ctx_ident = None;
    let mut params = Vec::new();

    for arg in &func.sig.inputs {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            continue;
        };
        let syn::Pat::Ident(pat_ident) = pat.as_ref() else {
            continue;
        };
        let ident = pat_ident.ident.clone();

        // Detect the `&Ctx` parameter used by the function macro contract.
        if is_ref_to(ty, "Ctx") {
            if ctx_ident.is_some() {
                return Err(SynError::new(
                    ident.span(),
                    "function must have exactly one `&Ctx` parameter",
                ));
            }
            ctx_ident = Some(ident);
            continue;
        }

        let is_ref = matches!(ty.as_ref(), Type::Reference(_));
        let is_str_ref = is_ref_to(ty, "str");
        params.push(ParamInfo {
            ident,
            ty: ty.as_ref().clone(),
            is_ref,
            is_str_ref,
        });
    }

    Ok((ctx_ident, params))
}

/// Like [`parse_fn_params_opt`], but requires a `&Ctx` parameter (memoized and
/// component-entry functions need one).
fn parse_fn_params(func: &ItemFn) -> syn::Result<(syn::Ident, Vec<ParamInfo>)> {
    let (ctx_ident, params) = parse_fn_params_opt(func)?;
    let ctx_ident = ctx_ident.ok_or_else(|| {
        SynError::new(
            func.sig.ident.span(),
            "function must have a `&Ctx` parameter",
        )
    })?;
    Ok((ctx_ident, params))
}

fn is_slice_ref(ty: &Type) -> bool {
    matches!(ty, Type::Reference(TypeReference { elem, .. }) if matches!(elem.as_ref(), Type::Slice(_)))
}

fn is_ref_to(ty: &Type, name: &str) -> bool {
    if let Type::Reference(TypeReference { elem, .. }) = ty
        && let Type::Path(type_path) = elem.as_ref()
    {
        return type_path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == name);
    }
    false
}

/// Generate clone statements for captured parameters.
fn gen_clones(params: &[ParamInfo]) -> Vec<TokenStream2> {
    params
        .iter()
        .map(|p| {
            let ident = &p.ident;
            if p.is_str_ref {
                quote! { let #ident = #ident.to_string(); }
            } else if is_slice_ref(&p.ty) {
                // `&[T]` has no owned `Clone`; its owned form is `Vec<T>`.
                quote! { let #ident = ::std::borrow::ToOwned::to_owned(#ident); }
            } else if p.is_ref {
                // For &T params, Clone::clone(param) gives T (owned)
                quote! { let #ident = ::core::clone::Clone::clone(#ident); }
            } else {
                // For owned params, param.clone() gives T (owned)
                quote! { let #ident = #ident.clone(); }
            }
        })
        .collect()
}

/// Compute a compile-time code hash (FNV-1a) of the function body's token stream.
/// If `version` is provided, it replaces the body hash as the canonical logic
/// representation, matching Python's `@coco.fn(version=...)` behavior.
fn compute_code_hash(block: &syn::Block, version: Option<u64>) -> u64 {
    let tokens = if let Some(version) = version {
        format!("<version>({version})")
    } else {
        block.to_token_stream().to_string()
    };
    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for byte in tokens.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV-1a prime
    }
    hash
}

/// Parsed arguments for `#[function(...)]`.
#[derive(Debug)]
struct FunctionArgs {
    memo: bool,
    batching: bool,
    max_batch_size: Option<usize>,
    version: Option<u64>,
    memo_key: Vec<MemoKeyOverride>,
    logic_tracking: LogicTracking,
}

/// `logic_tracking` mode, mirroring Python's `@coco.fn(logic_tracking=...)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicTracking {
    /// `"full"` (default): this function's own logic *and* the logic of every
    /// `#[function]` it transitively calls are tracked; a change in any of them
    /// invalidates memoized callers.
    Full,
    /// `"self"`: only this function's own body is tracked; logic changes in the
    /// functions it calls do not propagate up to it.
    SelfOnly,
    /// `"none"`: this function's logic is not tracked at all — it is not
    /// registered in the logic set and not recorded as a dependency of callers.
    None,
}

#[derive(Debug)]
struct MemoKeyOverride {
    ident: Ident,
    expr: Expr,
}

impl FunctionArgs {
    fn parse(attr: TokenStream2) -> syn::Result<Self> {
        syn::parse2(attr)
    }
}

impl Parse for FunctionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut memo = false;
        let mut batching = false;
        let mut max_batch_size = None;
        let mut version = None;
        let mut memo_key = Vec::new();
        let mut logic_tracking = LogicTracking::Full;

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            match name.to_string().as_str() {
                "memo" => memo = true,
                "batching" => batching = true,
                "max_batch_size" => {
                    input.parse::<Token![=]>()?;
                    let value: LitInt = input.parse()?;
                    if max_batch_size.is_some() {
                        return Err(SynError::new(
                            value.span(),
                            "duplicate `max_batch_size` argument",
                        ));
                    }
                    let value = value
                        .base10_parse::<usize>()
                        .map_err(|err| SynError::new(value.span(), err.to_string()))?;
                    if value == 0 {
                        return Err(SynError::new(
                            name.span(),
                            "`max_batch_size` must be greater than zero",
                        ));
                    }
                    max_batch_size = Some(value);
                }
                "logic_tracking" => {
                    input.parse::<Token![=]>()?;
                    let mode: LitStr = input.parse()?;
                    logic_tracking = match mode.value().as_str() {
                        "full" => LogicTracking::Full,
                        "self" => LogicTracking::SelfOnly,
                        "none" => LogicTracking::None,
                        other => {
                            return Err(SynError::new(
                                mode.span(),
                                format!(
                                    "invalid logic_tracking {other:?}; expected \"full\", \"self\", or \"none\""
                                ),
                            ));
                        }
                    };
                }
                "memo_key" => {
                    let content;
                    parenthesized!(content in input);
                    while !content.is_empty() {
                        let ident: Ident = content.parse()?;
                        content.parse::<Token![=]>()?;
                        let expr: Expr = content.parse()?;
                        memo_key.push(MemoKeyOverride { ident, expr });
                        if content.is_empty() {
                            break;
                        }
                        content.parse::<Token![,]>()?;
                        if content.is_empty() {
                            break;
                        }
                    }
                }
                "version" => {
                    input.parse::<Token![=]>()?;
                    let version_literal: LitInt = input.parse()?;
                    if version.is_some() {
                        return Err(SynError::new(
                            version_literal.span(),
                            "duplicate `version` argument",
                        ));
                    }
                    version =
                        Some(version_literal.base10_parse::<u64>().map_err(|err| {
                            SynError::new(version_literal.span(), err.to_string())
                        })?);
                }
                _ => {
                    return Err(SynError::new(
                        name.span(),
                        "unsupported function attribute argument. expected `memo`, `batching`, `max_batch_size = N`, `memo_key(...)`, `version = N`, or `logic_tracking = \"full\"|\"self\"|\"none\"`",
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
        }

        if max_batch_size.is_some() && !batching {
            return Err(SynError::new(
                Span::call_site(),
                "`max_batch_size` requires `batching`",
            ));
        }

        Ok(Self {
            memo,
            batching,
            max_batch_size,
            version,
            memo_key,
            logic_tracking,
        })
    }
}

fn is_skip_expr(expr: &Expr) -> bool {
    if let Expr::Path(path) = expr
        && path.qself.is_none()
    {
        return path
            .path
            .get_ident()
            .is_some_and(|ident| ident == "skip" || ident == "none" || ident == "None");
    }
    false
}

fn memo_key_override<'a>(
    overrides: &'a [MemoKeyOverride],
    ident: &Ident,
) -> Option<&'a MemoKeyOverride> {
    overrides.iter().find(|override_| override_.ident == *ident)
}

fn validate_memo_key_overrides(
    overrides: &[MemoKeyOverride],
    allowed: impl IntoIterator<Item = String>,
) -> syn::Result<()> {
    let allowed: std::collections::HashSet<String> = allowed.into_iter().collect();
    let mut seen = std::collections::HashSet::new();
    for override_ in overrides {
        let name = override_.ident.to_string();
        if !seen.insert(name.clone()) {
            return Err(SynError::new(
                override_.ident.span(),
                format!("duplicate memo_key override for `{name}`"),
            ));
        }
        if !allowed.contains(&name) {
            let mut allowed: Vec<_> = allowed.iter().cloned().collect();
            allowed.sort();
            return Err(SynError::new(
                override_.ident.span(),
                format!(
                    "unknown memo_key parameter `{name}`. expected one of: {}",
                    allowed.join(", ")
                ),
            ));
        }
    }
    Ok(())
}

fn gen_key_write_for_param(
    fingerprinter: &Ident,
    param: &ParamInfo,
    overrides: &[MemoKeyOverride],
) -> Option<TokenStream2> {
    let ident = &param.ident;
    let default_arg = if param.is_ref {
        quote! { #ident }
    } else {
        quote! { &#ident }
    };
    match memo_key_override(overrides, ident) {
        Some(override_) if is_skip_expr(&override_.expr) => None,
        Some(override_) => {
            let expr = &override_.expr;
            let temp = format_ident!("__coco_memo_key_{}", ident);
            Some(quote! {
                let #temp = (#expr)(#default_arg);
                ::cocoindex::memo::write_key_fingerprint_part(&mut #fingerprinter, &#temp)?;
            })
        }
        None if param.is_str_ref => Some(quote! {
            ::cocoindex::memo::write_key_fingerprint_part(&mut #fingerprinter, #default_arg)?;
        }),
        None => Some(quote! {
            ::cocoindex::memo::write_key_fingerprint_part_for_arg(&mut #fingerprinter, #default_arg)?;
        }),
    }
}

fn gen_state_collect_for_param(
    states_ident: &Ident,
    prev_states_ident: &Ident,
    param: &ParamInfo,
    overrides: &[MemoKeyOverride],
) -> TokenStream2 {
    if param.is_str_ref || memo_key_override(overrides, &param.ident).is_some() {
        return quote! {};
    }
    let ident = &param.ident;
    let default_arg = quote! { &#ident };
    quote! {
        let __coco_prev_state = #prev_states_ident
            .as_ref()
            .and_then(|__coco_previous_states| __coco_previous_states.get(#states_ident.len()));
        if let Some(__coco_state) =
            ::cocoindex::memo::collect_memo_arg_state(#default_arg, __coco_prev_state).await?
        {
            #states_ident.push(__coco_state);
        }
    }
}

fn gen_function_memo_key(
    fn_name: &Ident,
    hash_const_name: &Ident,
    key_writes: &[TokenStream2],
) -> TokenStream2 {
    quote! {{
        let mut __coco_key = ::cocoindex::memo::new_key_fingerprinter();
        ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &"cocoindex_fn")?;
        ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::module_path!())?;
        ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::stringify!(#fn_name))?;
        ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &#hash_const_name)?;
        #(#key_writes)*
        ::cocoindex::memo::finish_key_fingerprinter(__coco_key)
    }}
}

fn gen_memo_wrapper_block(
    fn_name: &Ident,
    hash_const_name: &Ident,
    ctx_ident: &Ident,
    params: &[ParamInfo],
    overrides: &[MemoKeyOverride],
    logic_tracking: LogicTracking,
    body_closure: TokenStream2,
) -> syn::Result<TokenStream2> {
    let propagate_children_fn_logic = logic_tracking == LogicTracking::Full;
    validate_memo_key_overrides(
        overrides,
        params.iter().map(|param| param.ident.to_string()),
    )?;
    let key_ident = format_ident!("__coco_key");
    let states_ident = format_ident!("__coco_states");
    let prev_states_ident = format_ident!("__coco_prev_states");
    let key_writes = params
        .iter()
        .filter_map(|param| gen_key_write_for_param(&key_ident, param, overrides))
        .collect::<Vec<_>>();
    let state_collects = params
        .iter()
        .map(|param| {
            gen_state_collect_for_param(&states_ident, &prev_states_ident, param, overrides)
        })
        .collect::<Vec<_>>();
    let state_params = params
        .iter()
        .filter(|param| !param.is_str_ref && memo_key_override(overrides, &param.ident).is_none())
        .cloned()
        .collect::<Vec<_>>();
    let state_clone_stmts = gen_clones(&state_params);
    let body_clone_stmts = gen_clones(params);
    let memo_key = gen_function_memo_key(fn_name, hash_const_name, &key_writes);
    // `logic_tracking = "none"` is never registered in the logic set, so its
    // fingerprint must not reach a caller's memo entry.
    let logic_fp = if logic_tracking != LogicTracking::None {
        quote! {
            ::core::option::Option::Some(::cocoindex::memo::fn_logic_fingerprint(
                ::core::module_path!(),
                ::core::stringify!(#fn_name),
                #hash_const_name,
            )?)
        }
    } else {
        quote! { ::core::option::Option::None }
    };

    Ok(quote! {{
        let __coco_key = #memo_key;
        let __coco_logic_fp = #logic_fp;

        ::cocoindex::memo::cached_by_fingerprint_with_state(
            #ctx_ident,
            __coco_key,
            __coco_logic_fp,
            #propagate_children_fn_logic,
            {
                #(#state_clone_stmts)*
                move |__coco_prev_states| async move {
                    let mut __coco_states = Vec::new();
                    #(#state_collects)*
                    Ok(__coco_states)
                }
            },
            {
                #(#body_clone_stmts)*
                #body_closure
            },
        )
        .await
    }})
}

#[derive(Debug)]
enum BatchImplParam {
    Context,
    Items,
    Extra(usize),
}

#[derive(Debug)]
struct BatchingSignature {
    ctx_ident: Ident,
    item_param: ParamInfo,
    extra_params: Vec<ParamInfo>,
    batch_impl_params: Vec<BatchImplParam>,
    output_ty: Type,
    wrapper_sig: syn::Signature,
}

fn vec_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

fn item_shaped_return_type(output: &syn::ReturnType) -> syn::Result<(Type, syn::ReturnType)> {
    let mut wrapper_output = output.clone();
    let invalid = || {
        SynError::new_spanned(
            output,
            "batching function return type must be `Result<Vec<_>>`",
        )
    };
    let syn::ReturnType::Type(_, wrapper_result_ty) = &mut wrapper_output else {
        return Err(invalid());
    };
    let Type::Path(wrapper_result_path) = wrapper_result_ty.as_mut() else {
        return Err(invalid());
    };
    let Some(wrapper_result_segment) = wrapper_result_path.path.segments.last_mut() else {
        return Err(invalid());
    };
    if wrapper_result_segment.ident != "Result" {
        return Err(invalid());
    }
    let syn::PathArguments::AngleBracketed(wrapper_args) = &mut wrapper_result_segment.arguments
    else {
        return Err(invalid());
    };
    let Some(syn::GenericArgument::Type(batch_output_ty)) = wrapper_args.args.first_mut() else {
        return Err(invalid());
    };
    let Some(item_output_ty) = vec_inner_type(batch_output_ty) else {
        return Err(invalid());
    };
    *batch_output_ty = item_output_ty.clone();
    Ok((item_output_ty, wrapper_output))
}

fn parse_batching_signature(func: &ItemFn) -> syn::Result<BatchingSignature> {
    if func.sig.asyncness.is_none() {
        return Err(SynError::new(
            func.sig.ident.span(),
            "batching requires an async function",
        ));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(SynError::new_spanned(
            &func.sig.generics,
            "batching functions cannot be generic because their scheduler is static",
        ));
    }
    let mut input_idents = Vec::with_capacity(func.sig.inputs.len());
    for input in &func.sig.inputs {
        let FnArg::Typed(param) = input else {
            return Err(SynError::new_spanned(
                input,
                "batching is supported only on free functions",
            ));
        };
        let Pat::Ident(param_ident) = param.pat.as_ref() else {
            return Err(SynError::new_spanned(
                &param.pat,
                "batching function parameters must be identifiers",
            ));
        };
        input_idents.push(param_ident.ident.clone());
    }

    let (ctx_ident, params) = parse_fn_params(func)?;
    let Some(batch_param) = params.first() else {
        return Err(SynError::new(
            func.sig.ident.span(),
            "batching requires a non-context `Vec<_>` parameter",
        ));
    };
    let Some(item_ty) = vec_inner_type(&batch_param.ty) else {
        return Err(SynError::new_spanned(
            &batch_param.ty,
            "batching requires the first non-context parameter to be `Vec<_>`",
        ));
    };
    let (output_ty, wrapper_output) = item_shaped_return_type(&func.sig.output)?;

    let mut wrapper_sig = func.sig.clone();
    let batch_input = wrapper_sig
        .inputs
        .iter_mut()
        .find_map(|input| match input {
            FnArg::Typed(param)
                if matches!(param.pat.as_ref(), Pat::Ident(ident) if ident.ident == batch_param.ident) =>
            {
                Some(param)
            }
            _ => None,
        })
        .expect("batch parameter was validated above");
    *batch_input.ty = item_ty.clone();
    wrapper_sig.output = wrapper_output;

    let batch_impl_params = input_idents
        .into_iter()
        .map(|ident| {
            if ident == ctx_ident {
                BatchImplParam::Context
            } else if ident == batch_param.ident {
                BatchImplParam::Items
            } else {
                let param_index = params
                    .iter()
                    .position(|param| param.ident == ident)
                    .expect("all non-context parameters were parsed");
                debug_assert!(param_index > 0);
                BatchImplParam::Extra(param_index - 1)
            }
        })
        .collect();

    Ok(BatchingSignature {
        ctx_ident,
        item_param: ParamInfo {
            ident: batch_param.ident.clone(),
            ty: item_ty,
            is_ref: false,
            is_str_ref: false,
        },
        extra_params: params[1..].to_vec(),
        batch_impl_params,
        output_ty,
        wrapper_sig,
    })
}

fn gen_owned_param_clones(params: &[ParamInfo]) -> Vec<TokenStream2> {
    params
        .iter()
        .map(|param| {
            let ident = &param.ident;
            quote! { let #ident = ::core::clone::Clone::clone(&#ident); }
        })
        .collect()
}

fn expand_batching_function(
    func: &ItemFn,
    args: &FunctionArgs,
    hash_const_name: &Ident,
    hash_const_value: &TokenStream2,
    logic_registration: &TokenStream2,
    track_logic: bool,
    propagate_children_fn_logic: bool,
) -> syn::Result<TokenStream2> {
    let batching = parse_batching_signature(func)?;
    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let attrs = &func.attrs;
    let body = &func.block;
    let wrapper_sig = &batching.wrapper_sig;
    let ctx_ident = &batching.ctx_ident;
    let item_ident = &batching.item_param.ident;
    let item_ty = &batching.item_param.ty;
    let output_ty = &batching.output_ty;
    let extra_params = &batching.extra_params;

    let batch_impl_name = format_ident!("__coco_batch_impl_{}", fn_name);
    let mut batch_impl_sig = func.sig.clone();
    batch_impl_sig.ident = batch_impl_name.clone();
    let batch_static_name = format_ident!("__COCO_BATCHED_{}", fn_name.to_string().to_uppercase());
    let max_batch_size = match args.max_batch_size {
        Some(max_batch_size) => quote! { ::core::option::Option::Some(#max_batch_size) },
        None => quote! { ::core::option::Option::None },
    };

    let batch_impl_args = batching.batch_impl_params.iter().map(|param| match param {
        BatchImplParam::Context => quote! { &__coco_batch_ctx },
        BatchImplParam::Items => quote! { __coco_batch_items },
        BatchImplParam::Extra(param_index) => {
            let param = &extra_params[*param_index];
            let ident = &param.ident;
            if param.is_ref {
                quote! { &#ident }
            } else {
                quote! { #ident }
            }
        }
    });

    let owned_extra_clones = gen_owned_param_clones(extra_params);
    let extra_key_parts = extra_params.iter().map(|param| {
        let ident = &param.ident;
        quote! { &#ident }
    });
    let extra_args_key = if extra_params.is_empty() {
        quote! { ::std::vec::Vec::new() }
    } else {
        quote! {
            ::cocoindex::memo::key_bytes_result(&(#(#extra_key_parts,)*))?
        }
    };
    let schedule_call = quote! {
        let __coco_extra_args_key = #extra_args_key;
        #batch_static_name
            .__call_scheduled(
                __coco_scoped_ctx,
                __coco_extra_args_key,
                #item_ident,
                move |__coco_batch_ctx, __coco_batch_items| {
                    #(#owned_extra_clones)*
                    async move {
                        #batch_impl_name(#(#batch_impl_args),*).await
                    }
                },
            )
            .await
    };

    let wrapper_body = if args.memo {
        let mut key_params = vec![batching.item_param.clone()];
        key_params.extend(extra_params.iter().cloned());
        gen_memo_wrapper_block(
            fn_name,
            hash_const_name,
            ctx_ident,
            &key_params,
            &args.memo_key,
            args.logic_tracking,
            quote! {
                move |__coco_scoped_ctx| async move {
                    #schedule_call
                }
            },
        )?
    } else {
        let initial_extra_clones = gen_clones(extra_params);
        if track_logic {
            quote! {{
                #(#initial_extra_clones)*
                #ctx_ident
                    .__coco_tracked_fn(
                        ::core::module_path!(),
                        ::core::stringify!(#fn_name),
                        #hash_const_name,
                        #propagate_children_fn_logic,
                        move |__coco_scoped_ctx| async move {
                            #schedule_call
                        },
                    )
                    .await
            }}
        } else {
            quote! {{
                #(#initial_extra_clones)*
                let __coco_scoped_ctx = ::core::clone::Clone::clone(#ctx_ident);
                #schedule_call
            }}
        }
    };

    Ok(quote! {
        #[doc(hidden)]
        pub const #hash_const_name: u64 = #hash_const_value;
        #logic_registration

        static #batch_static_name: ::std::sync::LazyLock<::cocoindex::batched::__ScheduledBatched<#item_ty, #output_ty>> =
            ::std::sync::LazyLock::new(|| {
                ::cocoindex::batched::__ScheduledBatched::__new(
                    #hash_const_name,
                    #max_batch_size,
                )
            });

        #batch_impl_sig #body

        #(#attrs)*
        #vis #wrapper_sig #wrapper_body
    })
}

/// `#[cocoindex::function]` — unified macro for cocoindex pipeline functions.
///
/// ## Usage
///
/// **Without arguments** — change tracking only (emits a code hash constant):
/// ```ignore
/// #[cocoindex::function]
/// async fn my_fn(ctx: &Ctx, arg: &str) -> Result<String> { ... }
/// ```
///
/// **With `memo`** — memoized computation:
/// ```ignore
/// #[cocoindex::function(memo)]
/// async fn my_fn(ctx: &Ctx, arg: &String) -> Result<String> { ... }
/// ```
/// The body receives a memo-scoped `Ctx`, so it can call `ctx.get_or_err()` or
/// `ctx.get_key()` directly. The attribute is the default way to memoize a
/// whole function because it includes the function's logic hash and arguments
/// in the key automatically.
///
/// Customize individual argument keys with `memo_key(...)`. A transform
/// function replaces the argument's default `cocoindex::MemoInput` identity
/// and suppresses its type-owned external-state validation. `skip` (or `None`)
/// excludes the argument entirely. A skipped parameter needs only `Clone`, not
/// `MemoInput` or `Serialize`:
/// ```ignore
/// #[cocoindex::function(memo, memo_key(client = skip))]
/// async fn fetch(ctx: &Ctx, url: &str, client: &ApiClient) -> Result<String> {
///     let configured_client = ctx.get_or_err::<ApiClient>()?;
///     configured_client.get(url).await
/// }
/// ```
///
/// Use `ctx.memo(...)` only for block-level memoization. Its closure body is not
/// logic-tracked; a manual key that should follow edits to the enclosing
/// `#[cocoindex::function]` must include that function's generated
/// `__COCO_FN_HASH_<NAME>` constant explicitly.
///
/// **With `batching`** — declare a batch-shaped body and call it with one item:
/// ```ignore
/// #[cocoindex::function(memo, batching, max_batch_size = 32)]
/// async fn embed(ctx: &Ctx, texts: Vec<String>) -> Result<Vec<Vec<f32>>> { ... }
///
/// let embedding: Vec<f32> = embed(ctx, text).await?;
/// ```
/// Additional parameters must implement `Serialize` because their serialized
/// values group compatible calls into a physical batch. With `memo`, they must
/// also implement `MemoInput` because they form part of each item's memo key,
/// unless `memo_key(...)` transforms or skips their memo identity.
///
/// Optional `version` parameter forces cache invalidation:
/// ```ignore
/// #[cocoindex::function(memo, version = 2)]
/// async fn my_fn(ctx: &Ctx, arg: &String) -> Result<String> { ... }
/// ```
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match FunctionArgs::parse(attr.into()) {
        Ok(args) => args,
        Err(err) => return TokenStream::from(err.to_compile_error()),
    };
    let func = parse_macro_input!(item as ItemFn);

    let code_hash = compute_code_hash(&func.block, args.version);
    let fn_name = &func.sig.ident;
    let hash_const_name = format_ident!("__COCO_FN_HASH_{}", fn_name.to_string().to_uppercase());

    // `logic_tracking` mode, lowered to two booleans:
    //  - `track_logic`: whether to register this function's logic fingerprint at
    //    all (`"full"`/`"self"` ⇒ yes, `"none"` ⇒ no).
    //  - `propagate_children_fn_logic`: whether a transitively-called
    //    `#[function]`'s logic change also invalidates this one (`"full"` only).
    let track_logic = args.logic_tracking != LogicTracking::None;
    let propagate_children_fn_logic = args.logic_tracking == LogicTracking::Full;

    // The `__COCO_FN_HASH_<NAME>` const feeds the memo key and the
    // `use_mount!`/`mount_each!` component fingerprint. For `logic_tracking =
    // "none"` the function's body must not be tracked anywhere (matching Python's
    // `None`), so the const is a fixed sentinel `0` rather than the body hash —
    // a body edit then invalidates neither the memo entry nor the component.
    // `module`/`name` still distinguish functions, so the sentinel can't collide.
    let hash_const_value = if track_logic {
        quote! { #code_hash }
    } else {
        quote! { 0u64 }
    };

    // Register this function's logic fingerprint into a link-time slice. At
    // app/environment startup the SDK drains the slice into the engine's logic
    // set, so a memoized caller's stored `logic_deps` (which include the
    // fingerprints of transitively-called `#[coco::function]`s) validate via
    // `all_contained` instead of being perpetually treated as stale. The
    // (module, name, code_hash) tuple must match `Ctx::__coco_tracked_fn`'s
    // fingerprint computation. `linkme` is reached through the re-export so the
    // user crate needs no direct dependency. Omitted entirely for
    // `logic_tracking = "none"`.
    let logic_slice_name = format_ident!("__COCO_FN_LOGIC_{}", fn_name.to_string().to_uppercase());
    let logic_registration = if track_logic {
        quote! {
            #[::cocoindex::linkme::distributed_slice(::cocoindex::COCO_FN_LOGIC)]
            #[linkme(crate = ::cocoindex::linkme)]
            #[doc(hidden)]
            static #logic_slice_name: ::cocoindex::FnLogicEntry = ::cocoindex::FnLogicEntry {
                module: ::core::module_path!(),
                name: ::core::stringify!(#fn_name),
                code_hash: #code_hash,
            };
        }
    } else {
        quote! {}
    };

    if !args.memo && !args.memo_key.is_empty() {
        return TokenStream::from(
            SynError::new(
                func.sig.ident.span(),
                "`memo_key(...)` requires `memo` because it customizes memoized inputs",
            )
            .to_compile_error(),
        );
    }

    if args.batching {
        return match expand_batching_function(
            &func,
            &args,
            &hash_const_name,
            &hash_const_value,
            &logic_registration,
            track_logic,
            propagate_children_fn_logic,
        ) {
            Ok(expanded) => expanded.into(),
            Err(err) => err.to_compile_error().into(),
        };
    }

    if args.memo {
        // memo: wrap body in ctx.memo()
        let (ctx_ident, params) = match parse_fn_params(&func) {
            Ok(params) => params,
            Err(err) => return TokenStream::from(err.to_compile_error()),
        };
        let vis = &func.vis;
        let sig = &func.sig;
        let attrs = &func.attrs;
        let body = &func.block;
        let wrapper_body = match gen_memo_wrapper_block(
            fn_name,
            &hash_const_name,
            &ctx_ident,
            &params,
            &args.memo_key,
            args.logic_tracking,
            // Bind the scoped context as `&Ctx`, matching the signature (and
            // the non-memo wrapper), so the body reads the same either way.
            quote! {
                move |__coco_scoped_ctx| async move {
                    let #ctx_ident = &__coco_scoped_ctx;
                    #body
                }
            },
        ) {
            Ok(wrapper) => wrapper,
            Err(err) => return TokenStream::from(err.to_compile_error()),
        };

        let expanded = quote! {
            #[doc(hidden)]
            pub const #hash_const_name: u64 = #hash_const_value;
            #logic_registration

            #(#attrs)*
            #vis #sig #wrapper_body
        };

        expanded.into()
    } else {
        // L0: change tracking only. With a `&Ctx`, the body runs every time but
        // its logic fingerprint and nested context deps propagate to memoized
        // callers (Python's `@coco.fn` without `memo=True`). Without a `&Ctx`,
        // the function is a pure logic-tracked helper — e.g. a batch impl for
        // `coco::Batched` — that gets a logic-hash const + registration only.
        let (ctx_opt, _) = match parse_fn_params_opt(&func) {
            Ok(params) => params,
            Err(err) => return TokenStream::from(err.to_compile_error()),
        };
        let vis = &func.vis;
        let sig = &func.sig;
        let attrs = &func.attrs;
        let body = &func.block;

        let expanded = if let Some(ctx_ident) = ctx_opt {
            if track_logic {
                quote! {
                    #[doc(hidden)]
                    pub const #hash_const_name: u64 = #hash_const_value;
                    #logic_registration

                    #(#attrs)*
                    #vis #sig {
                        #ctx_ident.__coco_tracked_fn(
                            ::core::module_path!(),
                            ::core::stringify!(#fn_name),
                            #hash_const_name,
                            #propagate_children_fn_logic,
                            move |__coco_scoped_ctx| async move {
                                let #ctx_ident = &__coco_scoped_ctx;
                                #body
                            },
                        ).await
                    }
                }
            } else {
                // `logic_tracking = "none"`: run the body without recording this
                // function's logic dependency. The hash const is still emitted so
                // `use_mount!`/`mount_each!` can reference it.
                quote! {
                    #[doc(hidden)]
                    pub const #hash_const_name: u64 = #hash_const_value;

                    #(#attrs)*
                    #vis #sig #body
                }
            }
        } else {
            quote! {
                #[doc(hidden)]
                pub const #hash_const_name: u64 = #hash_const_value;
                #logic_registration

                #(#attrs)*
                #vis #sig #body
            }
        };

        expanded.into()
    }
}

// ===========================================================================
// mount!/use_mount!/mount_each! — grouped-call mount macros (design.md §7.2)
//
// These read like the real call — `use_mount!(my_fn(ctx, a, b))` — and turn it
// into a child processing component whose component-memo fingerprint is
// `entry-fn logic ⊕ fingerprint(non-ctx args)`. The engine checks that
// fingerprint *before* running, so an unchanged component (and its whole
// subtree) is skipped. The macro relies on the `&Ctx`-is-always-first
// convention: the call's first argument is the parent ctx, which the macro
// replaces with the freshly-mounted child scope.
// ===========================================================================

/// Derive the `__COCO_FN_HASH_<NAME>` const path from a function path,
/// preserving the module prefix (`a::b::my_fn` → `a::b::__COCO_FN_HASH_MY_FN`).
fn hash_const_path(func_path: &Path) -> Path {
    let mut path = func_path.clone();
    if let Some(last) = path.segments.last_mut() {
        last.ident = format_ident!("__COCO_FN_HASH_{}", last.ident.to_string().to_uppercase());
        last.arguments = syn::PathArguments::None;
    }
    path
}

/// Unwrap a single tail-expression block (`{ expr }`) to `expr`. rustfmt may
/// rewrite a closure body `|x| call(...)` into block form `|x| { call(...) }`,
/// and users may write the block form by hand — both must still parse as a
/// grouped call.
fn unwrap_block_expr(expr: &Expr) -> &Expr {
    if let Expr::Block(block) = expr
        && block.block.stmts.len() == 1
        && let Stmt::Expr(inner, None) = &block.block.stmts[0]
    {
        return inner;
    }
    expr
}

/// Destructure a grouped call `my_fn(ctx, a, b)` into its path and arguments.
fn parse_grouped_call(expr: &Expr) -> syn::Result<(Path, Vec<Expr>)> {
    let expr = unwrap_block_expr(expr);
    let Expr::Call(call) = expr else {
        return Err(SynError::new_spanned(
            expr,
            "expected a function call like `my_fn(ctx, args...)`",
        ));
    };
    let Expr::Path(path) = call.func.as_ref() else {
        return Err(SynError::new_spanned(
            &call.func,
            "expected a plain function path",
        ));
    };
    let args: Vec<Expr> = call.args.iter().cloned().collect();
    if args.is_empty() {
        return Err(SynError::new_spanned(
            expr,
            "the call's first argument must be the `&Ctx`",
        ));
    }
    Ok((path.path.clone(), args))
}

/// `use_mount!(my_fn(ctx, a, b))` or `use_mount!(key, my_fn(ctx, a, b))`.
///
/// Foreground mount: creates a child component, runs `my_fn` under it with the
/// child `&Ctx` substituted for the first argument, and returns its value.
/// `(a, b)` are fingerprinted for the component-memo fast-path. The subpath is
/// `key` if given, else the function's name.
#[proc_macro]
pub fn use_mount(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input with Punctuated::<Expr, Token![,]>::parse_terminated);
    let elems: Vec<Expr> = parsed.into_iter().collect();
    let (key_expr, call) = match elems.as_slice() {
        [call] => (None, call),
        [key, call] => (Some(key), call),
        _ => {
            return SynError::new(
                Span::call_site(),
                "use_mount! takes `my_fn(ctx, args...)` or `key, my_fn(ctx, args...)`",
            )
            .to_compile_error()
            .into();
        }
    };

    let (path, args) = match parse_grouped_call(call) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let fn_ident = path.segments.last().unwrap().ident.clone();
    let hash_path = hash_const_path(&path);
    let ctx_arg = &args[0];
    let data_args = &args[1..];

    let arg_idents: Vec<Ident> = (0..data_args.len())
        .map(|i| format_ident!("__coco_arg{i}"))
        .collect();
    let arg_binds = arg_idents
        .iter()
        .zip(data_args)
        .map(|(id, expr)| quote! { let #id = #expr; });
    let fp_parts = arg_idents
        .iter()
        .map(|id| component_arg_part(quote! { & #id }));
    let key_tokens = match key_expr {
        Some(key) => quote! { ::std::string::ToString::to_string(&(#key)) },
        None => quote! { ::std::string::ToString::to_string(::core::stringify!(#fn_ident)) },
    };

    quote! {{
        use ::cocoindex::mount::{ComponentArgViaMemoInput as _, ComponentArgViaSerialize as _};
        let __coco_parent = &(#ctx_arg);
        #(#arg_binds)*
        let __coco_memo = ::cocoindex::mount::component_memo(
            ::core::module_path!(),
            ::core::stringify!(#fn_ident),
            #hash_path,
            ::std::vec![ #(#fp_parts),* ],
        )?;
        let __coco_key: ::std::string::String = #key_tokens;
        __coco_parent.__use_mount_fp(
            __coco_key,
            ::core::option::Option::Some(__coco_memo),
            move |__coco_child| async move {
                #path(&__coco_child, #(#arg_idents),*).await
            },
        )
    }}
    .into()
}

/// `mount_each!(items, |item| my_fn(ctx, item, a, b))` or
/// `mount_each!(prefix, items, |item| my_fn(ctx, item, a, b))`.
///
/// One child component per `(key, value)` item (subpath `prefix/key`, prefix
/// defaulting to the function name). The closure binds the item's **value**;
/// inside the call, `ctx` → child scope, the bound `item` → that value, and any
/// other arguments are captured extras that are fingerprinted (with the value)
/// for each item's component-memo key.
#[proc_macro]
pub fn mount_each(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input with Punctuated::<Expr, Token![,]>::parse_terminated);
    let elems: Vec<Expr> = parsed.into_iter().collect();
    let (prefix_expr, items_expr, closure) = match elems.as_slice() {
        [items, closure] => (None, items, closure),
        [prefix, items, closure] => (Some(prefix), items, closure),
        _ => {
            return SynError::new(
                Span::call_site(),
                "mount_each! takes `items, |item| my_fn(ctx, item, ...)` \
                 or `prefix, items, |item| my_fn(ctx, item, ...)`",
            )
            .to_compile_error()
            .into();
        }
    };

    let Expr::Closure(closure) = closure else {
        return SynError::new_spanned(closure, "expected a closure `|item| my_fn(ctx, item, ...)`")
            .to_compile_error()
            .into();
    };
    if closure.inputs.len() != 1 {
        return SynError::new_spanned(closure, "the closure must take exactly one item parameter")
            .to_compile_error()
            .into();
    }
    let item_ident = match &closure.inputs[0] {
        Pat::Ident(pat) => pat.ident.clone(),
        other => {
            return SynError::new_spanned(other, "the closure parameter must be an identifier")
                .to_compile_error()
                .into();
        }
    };

    let (path, args) = match parse_grouped_call(&closure.body) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let fn_ident = path.segments.last().unwrap().ident.clone();
    let hash_path = hash_const_path(&path);
    let ctx_arg = &args[0];

    // Classify each call argument: position 0 is the ctx; the argument equal to
    // the closure's item ident is the per-item value; everything else is a
    // captured extra (bound once, cloned per item).
    let mut extra_binds = Vec::new();
    let mut extra_idents = Vec::new();
    let mut fp_elems = Vec::new(); // non-ctx args, in order, for the memo fingerprint
    let mut body_args = Vec::new(); // all args, in order, for the body call
    for (i, arg) in args.iter().enumerate() {
        if i == 0 {
            body_args.push(quote! { &__coco_child });
            continue;
        }
        if expr_is_ident(arg, &item_ident) {
            fp_elems.push(component_arg_part(quote! { __coco_value }));
            body_args.push(quote! { __coco_value });
        } else {
            let id = format_ident!("__coco_extra{}", extra_idents.len());
            extra_binds.push(quote! { let #id = #arg; });
            fp_elems.push(component_arg_part(quote! { & #id }));
            body_args.push(quote! { #id });
            extra_idents.push(id);
        }
    }

    let prefix_tokens = match prefix_expr {
        Some(prefix) => quote! { ::core::option::Option::Some(#prefix) },
        None => quote! { ::core::option::Option::Some(::core::stringify!(#fn_ident)) },
    };

    // The memo and body closures are passed inline so their parameter types
    // are inferred from `items` before their bodies are checked: the memo
    // closure resolves `__coco_component_arg` by method lookup on the item's
    // concrete type.
    quote! {{
        use ::cocoindex::mount::{ComponentArgViaMemoInput as _, ComponentArgViaSerialize as _};
        #(#extra_binds)*
        (#ctx_arg).__mount_each_fp(
            #items_expr,
            #prefix_tokens,
            {
                #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
                move |__coco_value| -> ::cocoindex::Result<
                    ::core::option::Option<::cocoindex::mount::ComponentMemo>
                > {
                    ::cocoindex::mount::component_memo(
                        ::core::module_path!(),
                        ::core::stringify!(#fn_ident),
                        #hash_path,
                        ::std::vec![ #(#fp_elems),* ],
                    )
                    .map(::core::option::Option::Some)
                }
            },
            {
                #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
                move |__coco_child: ::cocoindex::Ctx, __coco_value| {
                    #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
                    async move { #path(#(#body_args),*).await }
                }
            },
        )
    }}
    .into()
}

/// One mount argument's memo part, chosen by autoref specialization: the
/// `MemoInput` path when the argument's type supports it, else `Serialize`.
fn component_arg_part(arg_ref: TokenStream2) -> TokenStream2 {
    quote! {
        (&&::cocoindex::mount::ComponentArg(#arg_ref)).__coco_component_arg()?
    }
}

/// Whether `expr` is exactly the identifier `ident` (a bare path with one
/// segment), used to spot the `mount_each!` item-value argument.
fn expr_is_ident(expr: &Expr, ident: &Ident) -> bool {
    if let Expr::Path(path) = expr
        && path.qself.is_none()
    {
        return path.path.is_ident(ident);
    }
    false
}

// ===========================================================================
// #[derive(SchemaFields)] — connector-agnostic table-schema derivation, the
// Rust analogue of Python's `TableSchema.from_class`.
// ===========================================================================

/// Per-field `#[coco(...)]` options.
#[derive(Default)]
struct SchemaFieldAttr {
    rename: Option<String>,
    custom_type: Option<String>,
    vector_dim: Option<u32>,
    vector_half: bool,
    force_json: bool,
}

fn parse_schema_field_attr(attrs: &[syn::Attribute]) -> syn::Result<SchemaFieldAttr> {
    let mut out = SchemaFieldAttr::default();
    let mut coco_attr = None;
    for attr in attrs {
        if !attr.path().is_ident("coco") {
            continue;
        }
        coco_attr = Some(attr);
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let v: syn::LitStr = meta.value()?.parse()?;
                out.rename = Some(v.value());
            } else if meta.path.is_ident("type") {
                let v: syn::LitStr = meta.value()?.parse()?;
                out.custom_type = Some(v.value());
            } else if meta.path.is_ident("vector") {
                out.vector_dim = Some(if meta.input.peek(Token![=]) {
                    let v: LitInt = meta.value()?.parse()?;
                    v.base10_parse()?
                } else {
                    // Zero is an unresolved sentinel. Connector schemas accept
                    // it only from `from_row` and resolve it through
                    // `with_vector_dim` before use.
                    0
                });
            } else if meta.path.is_ident("half") {
                out.vector_half = true;
            } else if meta.path.is_ident("json") {
                out.force_json = true;
            } else {
                return Err(meta.error("unknown #[coco(...)] option"));
            }
            Ok(())
        })?;
    }
    if let Some(attr) = coco_attr {
        let conflict = if out.vector_half && out.vector_dim.is_none() {
            Some("`half` applies only to a `vector` column; add `vector`")
        } else if out.custom_type.is_some()
            && (out.vector_dim.is_some() || out.vector_half || out.force_json)
        {
            Some(
                "`type` sets the column type itself; it cannot be combined with `vector`, `half` or `json`",
            )
        } else if out.force_json && out.vector_dim.is_some() {
            Some("a column cannot be both `json` and `vector`")
        } else {
            None
        };
        if let Some(message) = conflict {
            return Err(SynError::new_spanned(attr, message));
        }
    }
    Ok(out)
}

/// If `ty` is `Option<T>`, return `Some(T)`; otherwise `None`.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else { return None };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// The last path-segment identifier of a type, e.g. `i64`, `String`, `Vec`.
fn type_ident(ty: &Type) -> Option<String> {
    let Type::Path(tp) = ty else { return None };
    Some(tp.path.segments.last()?.ident.to_string())
}

/// The single generic argument's identifier, e.g. `u8` for `Vec<u8>`.
fn first_generic_ident(ty: &Type) -> Option<String> {
    let Type::Path(tp) = ty else { return None };
    let seg = tp.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => type_ident(t),
        _ => None,
    })
}

/// Map a (non-`Option`) Rust type to a `LogicalType` token expression, honoring
/// `#[coco(...)]` overrides. Mirrors the leaf-type dispatch in Python's
/// per-connector `_LEAF_TYPE_MAPPINGS`.
fn logical_type_tokens(ty: &Type, attr: &SchemaFieldAttr) -> TokenStream2 {
    if let Some(t) = &attr.custom_type {
        return quote! { ::cocoindex::LogicalType::Custom(::std::string::String::from(#t)) };
    }
    if let Some(dim) = attr.vector_dim {
        let half = attr.vector_half;
        return quote! { ::cocoindex::LogicalType::Vector { dim: #dim, half: #half } };
    }
    if attr.force_json {
        return quote! { ::cocoindex::LogicalType::Json };
    }
    let variant = match type_ident(ty).as_deref() {
        Some("bool") => quote! { Bool },
        Some("i8" | "i16") => quote! { Int16 },
        Some("i32") => quote! { Int32 },
        Some("i64" | "isize") => quote! { Int64 },
        Some("u8" | "u16") => quote! { Int32 },
        Some("u32" | "u64" | "usize") => quote! { Int64 },
        Some("f32") => quote! { Float32 },
        Some("f64") => quote! { Float64 },
        Some("String" | "str") => quote! { Text },
        Some("Uuid") => quote! { Uuid },
        Some("NaiveDate") => quote! { Date },
        Some("NaiveTime") => quote! { Time },
        Some("NaiveDateTime" | "DateTime") => quote! { DateTime },
        Some("Decimal") => quote! { Decimal },
        Some("Duration") => quote! { Duration },
        // `Vec<u8>` is bytes; any other `Vec<_>` falls through to JSON.
        Some("Vec") if first_generic_ident(ty).as_deref() == Some("u8") => quote! { Bytes },
        // Everything else (collections, maps, nested structs, enums) → JSON.
        _ => quote! { Json },
    };
    quote! { ::cocoindex::LogicalType::#variant }
}

/// `#[derive(SchemaFields)]` — derive a connector-agnostic table schema from a
/// row struct, the Rust analogue of Python's `TableSchema.from_class`. Pass the
/// type to a connector's `TableSchema::from_row::<T>(primary_key)`.
///
/// `Option<T>` fields become nullable columns; everything else is `NOT NULL`.
/// Use `#[coco(vector)]` with `TableSchema::with_vector_dim(...)` when the
/// dimension is available only at runtime, or `#[coco(vector = N)]` when it is
/// a compile-time constant. See [`cocoindex::row_schema`] for the other field
/// attributes.
///
/// Unsigned integers map to the next signed width, so `u64` and `usize` map to
/// a 64-bit integer column and a value above `i64::MAX` is rejected when the
/// row is written. Use `#[coco(type = "numeric")]` (or the connector's
/// equivalent) for the full unsigned range.
#[proc_macro_derive(SchemaFields, attributes(coco))]
pub fn derive_schema_fields(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let syn::Data::Struct(data) = &input.data else {
        return SynError::new_spanned(&input, "SchemaFields can only be derived for structs")
            .to_compile_error()
            .into();
    };
    let syn::Fields::Named(fields) = &data.fields else {
        return SynError::new_spanned(
            &data.fields,
            "SchemaFields requires a struct with named fields",
        )
        .to_compile_error()
        .into();
    };

    let mut entries = Vec::new();
    for field in &fields.named {
        let attr = match parse_schema_field_attr(&field.attrs) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error().into(),
        };
        let ident = field.ident.as_ref().expect("named field");
        let name_str = attr.rename.clone().unwrap_or_else(|| ident.to_string());

        let (base_ty, nullable) = match option_inner(&field.ty) {
            Some(inner) => (inner, true),
            None => (&field.ty, false),
        };
        let logical = logical_type_tokens(base_ty, &attr);
        entries.push(quote! {
            ::cocoindex::SchemaField {
                name: ::std::string::String::from(#name_str),
                logical_type: #logical,
                nullable: #nullable,
            }
        });
    }

    quote! {
        impl #impl_generics ::cocoindex::SchemaFields for #name #ty_generics #where_clause {
            fn schema_fields() -> ::std::vec::Vec<::cocoindex::SchemaField> {
                ::std::vec![ #(#entries),* ]
            }
        }
    }
    .into()
}

// ===========================================================================
// #[app] / #[lifespan] / #[main] — link-time registration for the CLI
//
// `#[cocoindex::app]` and `#[cocoindex::lifespan]` push one entry each into a
// `linkme` distributed slice (`::cocoindex::COCO_APPS` /
// `::cocoindex::COCO_LIFESPANS`), the mechanism `#[function]` uses for logic
// fingerprints. `Environment::from_registered()` and the `__coco` CLI protocol
// read those slices, so a user's `main` and the `cocoindex` CLI initialize the
// identical environment. `#[cocoindex::main]` runs the protocol dispatch before
// the user's own body.
// ===========================================================================

/// Parsed arguments for `#[app(...)]`: an optional `name = "..."`.
#[derive(Debug, Default)]
struct AppArgs {
    name: Option<LitStr>,
}

impl Parse for AppArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = AppArgs::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            if key == "name" {
                input.parse::<Token![=]>()?;
                let value: LitStr = input.parse()?;
                if value.value().is_empty() {
                    return Err(SynError::new(value.span(), "app name must not be empty"));
                }
                if args.name.replace(value).is_some() {
                    return Err(SynError::new(key.span(), "duplicate `name`"));
                }
            } else {
                return Err(SynError::new(
                    key.span(),
                    format!("unknown #[app] argument `{key}`; expected `name = \"...\"`"),
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

/// The single typed parameter of an `#[app]` / `#[lifespan]` function.
fn single_param<'a>(func: &'a ItemFn, expected: &str) -> syn::Result<&'a PatType> {
    let mut inputs = func.sig.inputs.iter();
    match (inputs.next(), inputs.next()) {
        (Some(FnArg::Typed(param)), None) => Ok(param),
        _ => Err(SynError::new(
            func.sig.ident.span(),
            format!("function must take exactly one parameter: {expected}"),
        )),
    }
}

fn require_async(func: &ItemFn, what: &str) -> syn::Result<()> {
    if func.sig.asyncness.is_none() {
        return Err(SynError::new(
            func.sig.fn_token.span,
            format!("{what} must be an `async fn`"),
        ));
    }
    Ok(())
}

fn type_ends_with(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(type_path)
        if type_path.path.segments.last().is_some_and(|segment| segment.ident == name))
}

/// Register an app entry point with the binary, so `cocoindex update` / `drop`
/// and [`Environment::from_registered`] can find it by name.
///
/// The function keeps its signature and stays callable (`app.run(app_main)`).
/// It must be `async fn name(ctx: Ctx) -> Result<T>` — `Ctx` by value. The
/// app's name is the function name unless `name = "..."` is given; names are
/// LMDB namespaces and must be unique per binary.
///
/// ```ignore
/// #[cocoindex::app(name = "TextEmbedding")]
/// async fn app_main(ctx: Ctx) -> Result<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn app(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<AppArgs>(attr) {
        Ok(args) => args,
        Err(err) => return TokenStream::from(err.to_compile_error()),
    };
    let func = parse_macro_input!(item as ItemFn);
    match expand_app(args, func) {
        Ok(tokens) => tokens.into(),
        Err(err) => TokenStream::from(err.to_compile_error()),
    }
}

fn expand_app(args: AppArgs, func: ItemFn) -> syn::Result<TokenStream2> {
    require_async(&func, "#[cocoindex::app] function")?;
    let param = single_param(&func, "`ctx: Ctx` (by value)")?;
    if !type_ends_with(&param.ty, "Ctx") {
        return Err(SynError::new_spanned(
            &param.ty,
            "#[cocoindex::app] function takes `ctx: Ctx` by value (not `&Ctx`)",
        ));
    }
    let fn_name = &func.sig.ident;
    let upper = fn_name.to_string().to_uppercase();
    let shim_name = format_ident!("__coco_app_{}", fn_name);
    let static_name = format_ident!("__COCO_APP_{}", upper);
    let app_name = match args.name {
        Some(lit) => quote! { #lit },
        None => quote! { ::core::stringify!(#fn_name) },
    };
    Ok(quote! {
        #func

        #[doc(hidden)]
        fn #shim_name(
            ctx: ::cocoindex::Ctx,
        ) -> ::cocoindex::__private::BoxFuture<'static, ::cocoindex::Result<()>> {
            ::std::boxed::Box::pin(async move { #fn_name(ctx).await.map(|_| ()) })
        }

        #[::cocoindex::linkme::distributed_slice(::cocoindex::COCO_APPS)]
        #[linkme(crate = ::cocoindex::linkme)]
        #[doc(hidden)]
        static #static_name: ::cocoindex::AppEntry = ::cocoindex::AppEntry {
            module: ::core::module_path!(),
            name: #app_name,
            run: #shim_name,
        };
    })
}

/// Register an environment lifespan with the binary. `Environment::from_registered`
/// (and so the `cocoindex` CLI) runs every lifespan over a fresh builder, then
/// opens the state db — the Rust analogue of Python's `@coco.lifespan`.
///
/// The function must be `async fn name(builder: EnvironmentBuilder) ->
/// Result<EnvironmentBuilder>`: configure the builder (`db_path`, `provide`,
/// `provide_key`, `on_shutdown` for teardown) and return it.
///
/// ```ignore
/// #[cocoindex::lifespan]
/// async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
///     let db = postgres::Database::connect(&url).await?;
///     Ok(builder.db_path("./.cocoindex_db").provide_key(&DB, db))
/// }
/// ```
#[proc_macro_attribute]
pub fn lifespan(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return TokenStream::from(
            SynError::new(
                Span::call_site(),
                "#[cocoindex::lifespan] takes no arguments",
            )
            .to_compile_error(),
        );
    }
    let func = parse_macro_input!(item as ItemFn);
    match expand_lifespan(func) {
        Ok(tokens) => tokens.into(),
        Err(err) => TokenStream::from(err.to_compile_error()),
    }
}

fn expand_lifespan(func: ItemFn) -> syn::Result<TokenStream2> {
    require_async(&func, "#[cocoindex::lifespan] function")?;
    let param = single_param(&func, "`builder: EnvironmentBuilder`")?;
    if !type_ends_with(&param.ty, "EnvironmentBuilder") {
        return Err(SynError::new_spanned(
            &param.ty,
            "#[cocoindex::lifespan] function takes `builder: EnvironmentBuilder` by value \
             and returns `Result<EnvironmentBuilder>`",
        ));
    }
    let fn_name = &func.sig.ident;
    let upper = fn_name.to_string().to_uppercase();
    let shim_name = format_ident!("__coco_lifespan_{}", fn_name);
    let static_name = format_ident!("__COCO_LIFESPAN_{}", upper);
    Ok(quote! {
        #func

        #[doc(hidden)]
        fn #shim_name(
            builder: ::cocoindex::EnvironmentBuilder,
        ) -> ::cocoindex::__private::BoxFuture<
            'static,
            ::cocoindex::Result<::cocoindex::EnvironmentBuilder>,
        > {
            ::std::boxed::Box::pin(#fn_name(builder))
        }

        #[::cocoindex::linkme::distributed_slice(::cocoindex::COCO_LIFESPANS)]
        #[linkme(crate = ::cocoindex::linkme)]
        #[doc(hidden)]
        static #static_name: ::cocoindex::LifespanEntry = ::cocoindex::LifespanEntry {
            module: ::core::module_path!(),
            name: ::core::stringify!(#fn_name),
            run: #shim_name,
        };
    })
}

/// Make an `async fn main` the entry point of a cocoindex binary.
///
/// The expansion builds a multi-thread tokio runtime, first lets the
/// `cocoindex` CLI's `__coco` protocol take over when the binary was started
/// by the CLI (`cocoindex::cli::try_dispatch`), and otherwise runs the body.
/// The body may return anything a plain `main` may return
/// ([`std::process::Termination`]: `()`, `ExitCode`, `Result<T, E: Debug>`, ...);
/// an `Err` is reported the same way. Use the async SDK
/// APIs inside; the `*_blocking` helpers would nest a runtime and panic.
///
/// ```ignore
/// #[cocoindex::main]
/// async fn main() -> cocoindex::Result<()> {
///     let env = Environment::from_registered().await?;
///     // ...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return TokenStream::from(
            SynError::new(Span::call_site(), "#[cocoindex::main] takes no arguments")
                .to_compile_error(),
        );
    }
    let func = parse_macro_input!(item as ItemFn);
    match expand_main(func) {
        Ok(tokens) => tokens.into(),
        Err(err) => TokenStream::from(err.to_compile_error()),
    }
}

fn expand_main(func: ItemFn) -> syn::Result<TokenStream2> {
    require_async(&func, "#[cocoindex::main] function")?;
    if !func.sig.inputs.is_empty() {
        return Err(SynError::new(
            func.sig.ident.span(),
            "#[cocoindex::main] function takes no parameters",
        ));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(SynError::new(
            func.sig.ident.span(),
            "#[cocoindex::main] function cannot be generic",
        ));
    }
    let attrs = &func.attrs;
    let vis = &func.vis;
    let name = &func.sig.ident;
    let mut inner = func.clone();
    inner.attrs.clear();
    inner.vis = syn::Visibility::Inherited;
    inner.sig.ident = format_ident!("__coco_user_main");
    Ok(quote! {
        #(#attrs)*
        #vis fn #name() -> ::std::process::ExitCode {
            #inner

            let runtime = match ::cocoindex::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                ::core::result::Result::Ok(runtime) => runtime,
                ::core::result::Result::Err(err) => {
                    ::std::eprintln!("cocoindex: failed to start the tokio runtime: {err}");
                    return ::std::process::ExitCode::FAILURE;
                }
            };
            let code = runtime.block_on(async {
                if let ::core::option::Option::Some(code) =
                    ::cocoindex::cli::try_dispatch().await
                {
                    return code;
                }
                ::std::process::Termination::report(__coco_user_main().await)
            });
            ::core::mem::drop(runtime);
            code
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    use syn::parse_str;

    fn item_fn(src: &str) -> ItemFn {
        parse_str::<ItemFn>(src).unwrap()
    }

    #[test]
    fn app_args_default_and_named() {
        let args: AppArgs = syn::parse2(TokenStream2::new()).unwrap();
        assert!(args.name.is_none());
        let args: AppArgs = syn::parse2(quote!(name = "TextEmbedding")).unwrap();
        assert_eq!(args.name.unwrap().value(), "TextEmbedding");
    }

    #[test]
    fn app_args_reject_unknown_and_empty() {
        let err = syn::parse2::<AppArgs>(quote!(title = "x")).unwrap_err();
        assert!(err.to_string().contains("unknown #[app] argument"));
        let err = syn::parse2::<AppArgs>(quote!(name = "")).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
        let err = syn::parse2::<AppArgs>(quote!(name = "a", name = "b")).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn app_expansion_registers_entry() {
        let func = item_fn("async fn app_main(ctx: Ctx) -> Result<u32> { Ok(1) }");
        let out = expand_app(AppArgs::default(), func).unwrap().to_string();
        assert!(out.contains("COCO_APPS"));
        assert!(out.contains("__COCO_APP_APP_MAIN"));
        assert!(out.contains("stringify ! (app_main)"));
        assert!(out.contains("async fn app_main"), "user fn is kept");
        let func = item_fn("async fn app_main(ctx: Ctx) -> Result<()> { Ok(()) }");
        let args: AppArgs = syn::parse2(quote!(name = "Named")).unwrap();
        let out = expand_app(args, func).unwrap().to_string();
        assert!(out.contains("name : \"Named\""));
    }

    #[test]
    fn app_rejects_wrong_shapes() {
        let err = expand_app(
            AppArgs::default(),
            item_fn("fn app_main(ctx: Ctx) -> Result<()> { Ok(()) }"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("async fn"));
        let err = expand_app(
            AppArgs::default(),
            item_fn("async fn app_main(ctx: &Ctx) -> Result<()> { Ok(()) }"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("by value"));
        let err = expand_app(
            AppArgs::default(),
            item_fn("async fn app_main(ctx: Ctx, n: u32) -> Result<()> { Ok(()) }"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("exactly one parameter"));
    }

    #[test]
    fn lifespan_expansion_and_errors() {
        let func = item_fn(
            "async fn init(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> { Ok(builder) }",
        );
        let out = expand_lifespan(func).unwrap().to_string();
        assert!(out.contains("COCO_LIFESPANS"));
        assert!(out.contains("__COCO_LIFESPAN_INIT"));
        let err = expand_lifespan(item_fn(
            "async fn init(builder: &mut EnvironmentBuilder) -> Result<()> { Ok(()) }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("by value"));
        let err = expand_lifespan(item_fn("async fn init() -> Result<()> { Ok(()) }")).unwrap_err();
        assert!(err.to_string().contains("exactly one parameter"));
    }

    #[test]
    fn main_expansion_and_errors() {
        let out = expand_main(item_fn("async fn main() -> Result<()> { Ok(()) }"))
            .unwrap()
            .to_string();
        assert!(out.contains("fn main () -> :: std :: process :: ExitCode"));
        assert!(out.contains("try_dispatch"));
        assert!(out.contains("__coco_user_main"));
        let err = expand_main(item_fn("fn main() {}")).unwrap_err();
        assert!(err.to_string().contains("async fn"));
        let err = expand_main(item_fn("async fn main(x: u32) {}")).unwrap_err();
        assert!(err.to_string().contains("no parameters"));
    }

    #[test]
    fn parse_function_args_empty() {
        let args = FunctionArgs::parse(TokenStream2::new()).unwrap();
        assert!(!args.memo);
        assert!(!args.batching);
        assert!(args.max_batch_size.is_none());
        assert!(args.version.is_none());
    }

    #[test]
    fn parse_function_args_accepts_batching_options() {
        let args = FunctionArgs::parse(quote!(memo, batching, max_batch_size = 32)).unwrap();
        assert!(args.memo);
        assert!(args.batching);
        assert_eq!(args.max_batch_size, Some(32));
    }

    #[test]
    fn parse_function_args_rejects_max_batch_without_batching() {
        let err = FunctionArgs::parse(quote!(max_batch_size = 32)).unwrap_err();
        assert!(err.to_string().contains("requires `batching`"));
    }

    #[test]
    fn parse_function_args_accepts_memo_key_overrides() {
        let args = FunctionArgs::parse(quote!(
            memo,
            memo_key(entry = normalize_entry, debug = skip)
        ))
        .unwrap();
        assert!(args.memo);
        assert_eq!(args.memo_key.len(), 2);
        assert_eq!(args.memo_key[0].ident, "entry");
        assert_eq!(args.memo_key[1].ident, "debug");
        assert!(is_skip_expr(&args.memo_key[1].expr));
    }

    #[test]
    fn memo_key_skip_accepts_python_style_none_alias() {
        let args = FunctionArgs::parse(quote!(memo, memo_key(debug = None))).unwrap();
        assert!(is_skip_expr(&args.memo_key[0].expr));
    }

    #[test]
    fn parse_function_args_rejects_unknown_flag() {
        let err = FunctionArgs::parse(quote!(memo, unknown)).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported function attribute argument")
        );
    }

    #[test]
    fn parse_function_args_rejects_bad_version() {
        let err = FunctionArgs::parse(quote!(memo, version = "x")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("integer") || msg.contains("digit"),
            "unexpected parse error: {msg}"
        );
    }

    #[test]
    fn memo_key_validation_rejects_duplicate_override() {
        let args = FunctionArgs::parse(quote!(memo, memo_key(entry = skip, entry = none))).unwrap();
        let err = validate_memo_key_overrides(&args.memo_key, ["entry".to_string()]).unwrap_err();
        assert!(err.to_string().contains("duplicate memo_key override"));
    }

    #[test]
    fn memo_key_validation_rejects_unknown_parameter() {
        let args =
            FunctionArgs::parse(quote!(memo, memo_key(entry = skip, missing = skip))).unwrap();
        let err = validate_memo_key_overrides(&args.memo_key, ["entry".to_string()]).unwrap_err();
        assert!(err.to_string().contains("unknown memo_key parameter"));
    }

    #[test]
    fn parse_fn_params_requires_ctx_reference() {
        let func: ItemFn =
            parse_str("async fn no_ctx(x: &str, value: i32) -> Result<i32, ()> { Ok(value) }")
                .unwrap();
        assert!(parse_fn_params(&func).is_err());
    }

    #[test]
    fn parse_fn_params_parses_ctx_and_params() {
        let func: ItemFn = parse_str(
            "async fn with_ctx(ctx: &Ctx, value: &str, count: usize) -> Result<i32, ()> { Ok(0) }",
        )
        .unwrap();
        let (ctx_ident, params) = parse_fn_params(&func).unwrap();
        assert_eq!(ctx_ident, "ctx");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn parse_fn_params_rejects_multiple_ctx_params() {
        let func: ItemFn = parse_str(
            "async fn bad(ctx: &Ctx, other: &cocoindex::Ctx) -> Result<i32, ()> { Ok(0) }",
        )
        .unwrap();
        assert!(parse_fn_params(&func).is_err());
    }

    #[test]
    fn parse_fn_params_rejects_ctx_like_suffix() {
        let func: ItemFn = parse_str(
            "async fn bad(value: &MyCtx, count: usize) -> Result<i32, ()> { Ok(count as i32) }",
        )
        .unwrap();
        assert!(parse_fn_params(&func).is_err());
    }

    #[test]
    fn parse_fn_params_accepts_qualified_ctx_type() {
        let func: ItemFn = parse_str(
            "async fn good(ctx: &cocoindex::Ctx, value: &str) -> Result<i32, ()> { Ok(0) }",
        )
        .unwrap();
        assert!(parse_fn_params(&func).is_ok());
    }

    #[test]
    fn parse_fn_params_accepts_local_qualified_ctx_type() {
        let func: ItemFn =
            parse_str("async fn good(ctx: &crate::Ctx, value: &str) -> Result<i32, ()> { Ok(0) }")
                .unwrap();
        assert!(parse_fn_params(&func).is_ok());
    }

    #[test]
    fn parse_fn_params_accepts_ctx_like_suffix_name() {
        let func: ItemFn = parse_str(
            "async fn bad(value: &utils::Ctx, count: usize) -> Result<i32, ()> { Ok(count as i32) }",
        )
        .unwrap();
        assert!(parse_fn_params(&func).is_ok());
        let func: ItemFn = parse_str(
            "async fn bad(value: &utils::MyCtx, count: usize) -> Result<i32, ()> { Ok(count as i32) }",
        )
        .unwrap();
        assert!(parse_fn_params(&func).is_err());
    }

    #[test]
    fn batching_signature_rewrites_input_and_output_to_item_shape() {
        let func: ItemFn = parse_str(
            "async fn embed(ctx: &Ctx, texts: Vec<String>, model: String) -> Result<Vec<Vec<f32>>> { unimplemented!() }",
        )
        .unwrap();
        let batching = parse_batching_signature(&func).unwrap();
        assert_eq!(batching.ctx_ident, "ctx");
        assert_eq!(batching.item_param.ident, "texts");
        assert_eq!(batching.extra_params.len(), 1);
        let wrapper_sig = &batching.wrapper_sig;
        assert_eq!(
            quote!(#wrapper_sig).to_string(),
            "async fn embed (ctx : & Ctx , texts : String , model : String) -> Result < Vec < f32 > >"
        );
    }

    #[test]
    fn batching_signature_requires_non_context_parameter() {
        let func: ItemFn =
            parse_str("async fn bad(ctx: &Ctx) -> Result<Vec<i32>> { Ok(vec![]) }").unwrap();
        let err = parse_batching_signature(&func).unwrap_err();
        assert!(err.to_string().contains("requires a non-context"));
    }

    #[test]
    fn batching_signature_requires_vec_input() {
        let func: ItemFn =
            parse_str("async fn bad(ctx: &Ctx, item: String) -> Result<Vec<i32>> { Ok(vec![]) }")
                .unwrap();
        let err = parse_batching_signature(&func).unwrap_err();
        assert!(err.to_string().contains("first non-context parameter"));
    }

    #[test]
    fn batching_signature_requires_result_vec_output() {
        let func: ItemFn =
            parse_str("async fn bad(ctx: &Ctx, items: Vec<String>) -> Result<i32> { Ok(1) }")
                .unwrap();
        let err = parse_batching_signature(&func).unwrap_err();
        assert!(err.to_string().contains("return type"));
    }

    #[test]
    fn explicit_version_replaces_body_hash() {
        let first: ItemFn =
            parse_str("async fn f(ctx: &Ctx) -> Result<i32, ()> { Ok(1) }").unwrap();
        let second: ItemFn =
            parse_str("async fn f(ctx: &Ctx) -> Result<i32, ()> { Ok(2) }").unwrap();

        assert_ne!(
            compute_code_hash(&first.block, None),
            compute_code_hash(&second.block, None)
        );
        assert_eq!(
            compute_code_hash(&first.block, Some(7)),
            compute_code_hash(&second.block, Some(7))
        );
    }

    #[test]
    fn explicit_version_changes_hash_when_bumped() {
        let func: ItemFn = parse_str("async fn f(ctx: &Ctx) -> Result<i32, ()> { Ok(1) }").unwrap();

        assert_ne!(
            compute_code_hash(&func.block, Some(7)),
            compute_code_hash(&func.block, Some(8))
        );
    }

    #[test]
    fn batching_memo_key_changes_with_body() {
        let first: ItemFn = parse_str(
            "async fn embed(ctx: &Ctx, items: Vec<i32>) -> Result<Vec<i32>> { Ok(items) }",
        )
        .unwrap();
        let second: ItemFn = parse_str(
            "async fn embed(ctx: &Ctx, items: Vec<i32>) -> Result<Vec<i32>> { \
             Ok(items.into_iter().map(|item| item * 2).collect()) }",
        )
        .unwrap();
        assert_ne!(
            compute_code_hash(&first.block, None),
            compute_code_hash(&second.block, None),
            "editing a batching body must change its generated logic hash"
        );

        let args = FunctionArgs::parse(quote!(memo, batching)).unwrap();
        let hash_const_name = format_ident!("__COCO_FN_HASH_EMBED");
        let first_hash = compute_code_hash(&first.block, None);
        let second_hash = compute_code_hash(&second.block, None);
        let first_expanded = expand_batching_function(
            &first,
            &args,
            &hash_const_name,
            &quote!(#first_hash),
            &quote!(),
            true,
            true,
        )
        .unwrap()
        .to_string();
        let second_expanded = expand_batching_function(
            &second,
            &args,
            &hash_const_name,
            &quote!(#second_hash),
            &quote!(),
            true,
            true,
        )
        .unwrap()
        .to_string();
        let key_part = "write_key_fingerprint_part (& mut __coco_key , & __COCO_FN_HASH_EMBED)";
        assert!(first_expanded.contains(key_part));
        assert!(second_expanded.contains(key_part));
        assert!(first_expanded.contains(&first_hash.to_string()));
        assert!(second_expanded.contains(&second_hash.to_string()));
        assert_ne!(
            first_hash, second_hash,
            "two batching bodies must produce different memo-key constants"
        );
    }
}
