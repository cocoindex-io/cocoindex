//! Two `#[cocoindex::app]`s with one name: names are LMDB namespaces, so the
//! registry refuses them and the error names both functions' modules.

use cocoindex::prelude::*;

mod first {
    use cocoindex::prelude::*;

    #[cocoindex::app(name = "Dup")]
    pub async fn one(_ctx: Ctx) -> Result<()> {
        Ok(())
    }
}

mod second {
    use cocoindex::prelude::*;

    #[cocoindex::app(name = "Dup")]
    pub async fn two(_ctx: Ctx) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn duplicate_app_names_are_refused() {
    let message = cocoindex::registry::check_unique_app_names()
        .expect_err("two apps named Dup")
        .to_string();
    assert!(message.contains("`Dup`"), "{message}");
    assert!(message.contains("registered_dup::first"), "{message}");
    assert!(message.contains("registered_dup::second"), "{message}");

    // SAFETY: single test in this binary.
    unsafe { std::env::set_var("COCOINDEX_DB", tempfile::tempdir().unwrap().keep()) };
    let err = Environment::from_registered().await.err().expect("refused");
    assert!(err.to_string().contains("`Dup`"), "{err}");
}
