mod base;
mod builder;
mod execution;
mod lib_context;
mod llm;
mod ops;
mod py;
mod server;
mod service;
mod settings;
mod setup;
mod utils;

use lib_context::LibContext;
use std::sync::{Arc, RwLock};

static LIB_CONTEXT: RwLock<Option<Arc<LibContext>>> = RwLock::new(None);

pub(crate) fn get_lib_context() -> Option<Arc<LibContext>> {
    let lib_context_locked = LIB_CONTEXT.read().unwrap();
    lib_context_locked.as_ref().cloned()
}
