//! Proc macros for cocoindex: `#[function]`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    Error as SynError, Expr, FnArg, Ident, ItemFn, LitInt, LitStr, Pat, PatType, Path, Stmt, Token,
    Type, TypeReference, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

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
        if is_ctx_type(ty) {
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
        let is_str_ref = is_str_ref_type(ty);
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

fn is_str_ref_type(ty: &Type) -> bool {
    if let Type::Reference(TypeReference { elem, .. }) = ty
        && let Type::Path(type_path) = elem.as_ref()
    {
        return type_path
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "str");
    }
    false
}

/// Check if a type is a `&Ctx` reference.
fn is_ctx_type(ty: &Type) -> bool {
    if let Type::Reference(TypeReference { elem, .. }) = ty
        && let Type::Path(type_path) = elem.as_ref()
    {
        return type_path
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "Ctx");
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

use proc_macro2::TokenStream as Pm2TokenStream;

trait ToTokenStream {
    fn to_token_stream(&self) -> Pm2TokenStream;
}

impl ToTokenStream for syn::Block {
    fn to_token_stream(&self) -> Pm2TokenStream {
        quote! { #self }
    }
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
    state_idx_ident: &Ident,
    prev_states_ident: &Ident,
    param: &ParamInfo,
) -> TokenStream2 {
    if param.is_str_ref {
        return quote! {};
    }
    let ident = &param.ident;
    let default_arg = quote! { &#ident };
    quote! {
        let __coco_prev_state = #prev_states_ident
            .as_ref()
            .and_then(|__coco_states| __coco_states.get(#state_idx_ident));
        if let Some(__coco_state) =
            ::cocoindex::memo::collect_memo_arg_state(#default_arg, __coco_prev_state).await?
        {
            #states_ident.push(__coco_state);
            #state_idx_ident += 1;
        }
    }
}

#[derive(Debug)]
struct BatchingSignature {
    ctx_ident: Ident,
    item_param: ParamInfo,
    extra_params: Vec<ParamInfo>,
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
    let syn::ReturnType::Type(_, result_ty) = output else {
        return Err(SynError::new_spanned(
            output,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };
    let Type::Path(result_path) = result_ty.as_ref() else {
        return Err(SynError::new_spanned(
            result_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };
    let Some(result_segment) = result_path.path.segments.last() else {
        return Err(SynError::new_spanned(
            result_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };
    if result_segment.ident != "Result" {
        return Err(SynError::new_spanned(
            result_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    }
    let syn::PathArguments::AngleBracketed(result_args) = &result_segment.arguments else {
        return Err(SynError::new_spanned(
            result_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };
    let Some(syn::GenericArgument::Type(batch_output_ty)) = result_args.args.first() else {
        return Err(SynError::new_spanned(
            result_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };
    let Some(item_output_ty) = vec_inner_type(batch_output_ty) else {
        return Err(SynError::new_spanned(
            batch_output_ty,
            "batching function return type must be `Result<Vec<_>>`",
        ));
    };

    let mut wrapper_output = output.clone();
    let syn::ReturnType::Type(_, wrapper_result_ty) = &mut wrapper_output else {
        unreachable!();
    };
    let Type::Path(wrapper_result_path) = wrapper_result_ty.as_mut() else {
        unreachable!();
    };
    let wrapper_result_segment = wrapper_result_path.path.segments.last_mut().unwrap();
    let syn::PathArguments::AngleBracketed(wrapper_args) = &mut wrapper_result_segment.arguments
    else {
        unreachable!();
    };
    let Some(syn::GenericArgument::Type(wrapper_item_ty)) = wrapper_args.args.first_mut() else {
        unreachable!();
    };
    *wrapper_item_ty = item_output_ty.clone();
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
    for input in &func.sig.inputs {
        let FnArg::Typed(param) = input else {
            return Err(SynError::new_spanned(
                input,
                "batching is supported only on free functions",
            ));
        };
        if !matches!(param.pat.as_ref(), Pat::Ident(_)) {
            return Err(SynError::new_spanned(
                &param.pat,
                "batching function parameters must be identifiers",
            ));
        }
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
    let mut found_batch_param = false;
    for input in &mut wrapper_sig.inputs {
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
        if param_ident.ident == batch_param.ident {
            *param.ty = item_ty.clone();
            found_batch_param = true;
            break;
        }
    }
    debug_assert!(found_batch_param);
    wrapper_sig.output = wrapper_output;

    Ok(BatchingSignature {
        ctx_ident,
        item_param: ParamInfo {
            ident: batch_param.ident.clone(),
            ty: item_ty,
            is_ref: false,
            is_str_ref: false,
        },
        extra_params: params[1..].to_vec(),
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

#[allow(clippy::too_many_arguments)]
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
    let batch_static_init = match args.max_batch_size {
        Some(max_batch_size) => quote! {
            ::cocoindex::Batched::__scheduled_with_max_batch(
                #hash_const_name,
                #max_batch_size,
            )
        },
        None => quote! { ::cocoindex::Batched::__scheduled(#hash_const_name) },
    };

    let mut batch_impl_args = Vec::new();
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
        let ident = &param_ident.ident;
        if *ident == batching.ctx_ident {
            batch_impl_args.push(quote! { &__coco_batch_ctx });
        } else if *ident == batching.item_param.ident {
            batch_impl_args.push(quote! { __coco_batch_items });
        } else {
            let param = extra_params
                .iter()
                .find(|param| param.ident == *ident)
                .expect("validated non-context parameter");
            if param.is_ref {
                batch_impl_args.push(quote! { &#ident });
            } else {
                batch_impl_args.push(quote! { #ident });
            }
        }
    }

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
        validate_memo_key_overrides(
            &args.memo_key,
            key_params.iter().map(|param| param.ident.to_string()),
        )?;
        let key_writes: Vec<TokenStream2> = key_params
            .iter()
            .filter_map(|param| {
                gen_key_write_for_param(&format_ident!("__coco_key"), param, &args.memo_key)
            })
            .collect();
        let state_collects: Vec<TokenStream2> = key_params
            .iter()
            .map(|param| {
                gen_state_collect_for_param(
                    &format_ident!("__coco_states"),
                    &format_ident!("__coco_state_idx"),
                    &format_ident!("__coco_prev_states"),
                    param,
                )
            })
            .collect();
        let state_clones = gen_clones(&key_params);
        let body_clones = gen_clones(&key_params);
        quote! {{
            let __coco_key = {
                let mut __coco_key = ::cocoindex::memo::new_key_fingerprinter();
                ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &"cocoindex_fn")?;
                ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::module_path!())?;
                ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::stringify!(#fn_name))?;
                ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &#hash_const_name)?;
                #(#key_writes)*
                ::cocoindex::memo::finish_key_fingerprinter(__coco_key)
            };

            ::cocoindex::memo::cached_by_fingerprint_with_state(
                #ctx_ident,
                __coco_key,
                #propagate_children_fn_logic,
                {
                    #(#state_clones)*
                    move |__coco_prev_states| async move {
                        let mut __coco_states = Vec::new();
                        let mut __coco_state_idx = 0usize;
                        #(#state_collects)*
                        Ok(__coco_states)
                    }
                },
                {
                    #(#body_clones)*
                    move |__coco_scoped_ctx| async move {
                        #schedule_call
                    }
                },
            )
            .await
        }}
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

        static #batch_static_name: ::std::sync::LazyLock<::cocoindex::Batched<#item_ty, #output_ty>> =
            ::std::sync::LazyLock::new(|| #batch_static_init);

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
/// function replaces the argument's default representation; `skip` (or `None`)
/// excludes it entirely. A skipped parameter needs only `Any + Clone`, not
/// `Serialize`:
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
        let clone_stmts = gen_clones(&params);
        if let Err(err) =
            validate_memo_key_overrides(&args.memo_key, params.iter().map(|p| p.ident.to_string()))
        {
            return TokenStream::from(err.to_compile_error());
        }

        let key_writes: Vec<TokenStream2> = params
            .iter()
            .filter_map(|p| {
                gen_key_write_for_param(&format_ident!("__coco_key"), p, &args.memo_key)
            })
            .collect();
        let state_collects: Vec<TokenStream2> = params
            .iter()
            .map(|p| {
                gen_state_collect_for_param(
                    &format_ident!("__coco_states"),
                    &format_ident!("__coco_state_idx"),
                    &format_ident!("__coco_prev_states"),
                    p,
                )
            })
            .collect();

        let expanded = quote! {
            #[doc(hidden)]
            pub const #hash_const_name: u64 = #hash_const_value;
            #logic_registration

            #(#attrs)*
            #vis #sig {
                let __coco_key = {
                    let mut __coco_key = ::cocoindex::memo::new_key_fingerprinter();
                    ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &"cocoindex_fn")?;
                    ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::module_path!())?;
                    ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &::core::stringify!(#fn_name))?;
                    ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &#hash_const_name)?;
                    #(#key_writes)*
                    ::cocoindex::memo::finish_key_fingerprinter(__coco_key)
                };

                ::cocoindex::memo::cached_by_fingerprint_with_state(#ctx_ident, __coco_key, #propagate_children_fn_logic, {
                    #(#clone_stmts)*
                    move |__coco_prev_states| async move {
                        let mut __coco_states = Vec::new();
                        let mut __coco_state_idx = 0usize;
                        #(#state_collects)*
                        Ok(__coco_states)
                    }
                }, {
                    #(#clone_stmts)*
                    move |#ctx_ident| async move #body
                }).await
            }
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
            // No `&Ctx`: a pure logic-tracked helper, typically a batch impl for
            // `coco::Batched`.
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
    let fp_refs = arg_idents.iter().map(|id| quote! { & #id });
    let key_tokens = match key_expr {
        Some(key) => quote! { ::std::string::ToString::to_string(&(#key)) },
        None => quote! { ::std::string::ToString::to_string(::core::stringify!(#fn_ident)) },
    };

    quote! {{
        let __coco_parent = &(#ctx_arg);
        #(#arg_binds)*
        let __coco_memo_fp = ::cocoindex::mount::component_memo_fp(
            ::core::module_path!(),
            ::core::stringify!(#fn_ident),
            #hash_path,
            &( #(#fp_refs,)* ),
        )?;
        let __coco_key: ::std::string::String = #key_tokens;
        __coco_parent.__use_mount_fp(
            __coco_key,
            ::core::option::Option::Some(__coco_memo_fp),
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
            fp_elems.push(quote! { __coco_value });
            body_args.push(quote! { __coco_value });
        } else {
            let id = format_ident!("__coco_extra{}", extra_idents.len());
            extra_binds.push(quote! { let #id = #arg; });
            fp_elems.push(quote! { & #id });
            body_args.push(quote! { #id });
            extra_idents.push(id);
        }
    }

    let prefix_tokens = match prefix_expr {
        Some(prefix) => quote! { ::core::option::Option::Some(#prefix) },
        None => quote! { ::core::option::Option::Some(::core::stringify!(#fn_ident)) },
    };

    quote! {{
        #(#extra_binds)*
        let __coco_memo_of = {
            #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
            move |__coco_value: &_| ::cocoindex::mount::component_memo_fp(
                ::core::module_path!(),
                ::core::stringify!(#fn_ident),
                #hash_path,
                &( #(#fp_elems,)* ),
            )
            .map(::core::option::Option::Some)
        };
        let __coco_body = {
            #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
            move |__coco_child: ::cocoindex::Ctx, __coco_value| {
                #( let #extra_idents = ::core::clone::Clone::clone(&#extra_idents); )*
                async move { #path(#(#body_args),*).await }
            }
        };
        (#ctx_arg).__mount_each_fp(#items_expr, #prefix_tokens, __coco_memo_of, __coco_body)
    }}
    .into()
}

/// Whether `expr` is exactly the identifier `ident` (a bare path with one
/// segment), used to spot the `mount_each!` item-value argument.
fn expr_is_ident(expr: &Expr, ident: &Ident) -> bool {
    if let Expr::Path(path) = expr {
        if path.qself.is_none() {
            return path.path.is_ident(ident);
        }
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
    for attr in attrs {
        if !attr.path().is_ident("coco") {
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    use syn::parse_str;

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
}
