//! `sdk_version` is this crate's package version and git commit (E06-D008).
//!
//! vergen-gitcl is the vergen git backend. It emits `VERGEN_GIT_SHA`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gitcl = vergen_gitcl::Gitcl::builder().sha(false).build();
    vergen_gitcl::Emitter::default()
        .add_instructions(&gitcl)?
        .emit()?;
    Ok(())
}
