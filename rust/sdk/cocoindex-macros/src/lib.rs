//! Proc macros for cocoindex: `#[function]`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Error as SynError, FnArg, Ident, ItemFn, LitInt, PatType, Token, Type, TypeReference,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// Information about a non-ctx parameter.
struct ParamInfo {
    ident: syn::Ident,
    is_ref: bool,
}

/// Parse an async fn and extract the ctx parameter name + non-ctx parameter info.
fn parse_fn_params(func: &ItemFn) -> syn::Result<(syn::Ident, Vec<ParamInfo>)> {
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
        params.push(ParamInfo { ident, is_ref });
    }

    let ctx_ident = ctx_ident.ok_or_else(|| {
        SynError::new(
            func.sig.ident.span(),
            "function must have a `&Ctx` parameter",
        )
    })?;
    Ok((ctx_ident, params))
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
            if p.is_ref {
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
/// If `version` is provided, it is mixed into the hash.
fn compute_code_hash(block: &syn::Block, version: Option<u64>) -> u64 {
    let tokens = block.to_token_stream().to_string();
    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for byte in tokens.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV-1a prime
    }
    if let Some(v) = version {
        // Mix in version
        for byte in v.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
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
    version: Option<u64>,
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
        let mut version = None;

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            match name.to_string().as_str() {
                "memo" => memo = true,
                "batching" => batching = true,
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
                        "unsupported function attribute argument. expected `memo`, `batching`, or `version = N`",
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

        Ok(Self {
            memo,
            batching,
            version,
        })
    }
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
///
/// **With `batching`** — batch processing (no caching, body gets all items every time):
/// ```ignore
/// #[cocoindex::function(batching)]
/// async fn my_fn(ctx: &Ctx, items: Vec<FileEntry>) -> Result<Vec<Info>> {
///     // `items` is always the full list — no caching
/// }
/// ```
///
/// **With `memo, batching`** — per-item memoization + batch execution:
/// ```ignore
/// #[cocoindex::function(memo, batching)]
/// async fn my_fn(ctx: &Ctx, items: Vec<FileEntry>) -> Result<Vec<Info>> {
///     // `items` here is only the cache misses
///     // `ctx` is available (unlike plain `memo`)
/// }
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

    if args.memo && args.batching {
        // memo + batching: wrap body in ctx.batch() with per-item memoization
        let (ctx_ident, params) = match parse_fn_params(&func) {
            Ok(params) => params,
            Err(err) => return TokenStream::from(err.to_compile_error()),
        };

        if params.is_empty() {
            return TokenStream::from(
                SynError::new(
                    func.sig.ident.span(),
                    "#[cocoindex::function(memo, batching)]: function must have at least one non-ctx parameter (the items)",
                )
                .to_compile_error(),
            );
        }

        // First non-ctx param is the items collection.
        let items_param = &params[0];
        let items_ident = &items_param.ident;

        // Remaining params are "extra" — cloned into closure, included in key.
        let extra_params = &params[1..];
        let clone_stmts = gen_clones(extra_params);

        let extra_key_writes: Vec<TokenStream2> = extra_params
            .iter()
            .map(|p| {
                let ident = &p.ident;
                if p.is_ref {
                    quote! { ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key_prefix, #ident)?; }
                } else {
                    quote! { ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key_prefix, &#ident)?; }
                }
            })
            .collect();

        let vis = &func.vis;
        let sig = &func.sig;
        let attrs = &func.attrs;
        let body = &func.block;

        let expanded = quote! {
            #[doc(hidden)]
            pub const #hash_const_name: u64 = #code_hash;

            #(#attrs)*
            #vis #sig {
                let mut __coco_key_prefix = ::cocoindex::memo::new_key_fingerprinter();
                ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key_prefix, &#hash_const_name)?;
                #(#extra_key_writes)*

                ::cocoindex::memo::batch_by_fingerprint(
                    #ctx_ident,
                    #items_ident,
                    |__coco_item| {
                        let mut __coco_key = __coco_key_prefix.clone();
                        ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, __coco_item)?;
                        Ok(::cocoindex::memo::finish_key_fingerprinter(__coco_key))
                    },
                    {
                        #(#clone_stmts)*
                        move |#items_ident| async move #body
                    },
                ).await
            }
        };

        expanded.into()
    } else if args.memo {
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

        let key_writes: Vec<TokenStream2> = params
            .iter()
            .map(|p| {
                let ident = &p.ident;
                if p.is_ref {
                    quote! { ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, #ident)?; }
                } else {
                    quote! { ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &#ident)?; }
                }
            })
            .collect();

        let expanded = quote! {
            #[doc(hidden)]
            pub const #hash_const_name: u64 = #code_hash;

            #(#attrs)*
            #vis #sig {
                let __coco_key = {
                    let mut __coco_key = ::cocoindex::memo::new_key_fingerprinter();
                    ::cocoindex::memo::write_key_fingerprint_part(&mut __coco_key, &#hash_const_name)?;
                    #(#key_writes)*
                    ::cocoindex::memo::finish_key_fingerprinter(__coco_key)
                };

                ::cocoindex::memo::cached_by_fingerprint(#ctx_ident, __coco_key, {
                    #(#clone_stmts)*
                    move || async move #body
                }).await
            }
        };

        expanded.into()
    } else {
        // L0: emit hash constant + original function unchanged
        let vis = &func.vis;
        let sig = &func.sig;
        let attrs = &func.attrs;
        let body = &func.block;

        let expanded = quote! {
            #[doc(hidden)]
            pub const #hash_const_name: u64 = #code_hash;

            #(#attrs)*
            #vis #sig #body
        };

        expanded.into()
    }
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
        assert!(args.version.is_none());
    }

    #[test]
    fn parse_function_args_batching_memo_with_version() {
        let args = FunctionArgs::parse(quote!(memo, batching, version = 42)).unwrap();
        assert!(args.memo);
        assert!(args.batching);
        assert_eq!(args.version, Some(42));
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
}
