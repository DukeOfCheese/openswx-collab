//! `openswx-collab-server`: the open-source collaboration server, with no extensions.

use std::process::ExitCode;
use std::sync::Arc;

use oswx_server::ext::NoExtensions;

fn main() -> ExitCode {
    oswx_serverd::run(Arc::new(NoExtensions))
}
