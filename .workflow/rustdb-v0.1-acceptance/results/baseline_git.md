# Baseline Git result

Accepted:

- Initial commit `74194ab` contains the verified v0.1 engine and 112-test
  acceptance state.
- Annotated local tag `v0.1.0-alpha.1` resolves to that commit.
- Repository-local identity `RustDB Builder <rustdb@localhost>` was used; no
  global Git configuration or remote was changed.

Verification:

- `git diff --cached --check` passed before the commit.
- `git show --no-patch --oneline --decorate HEAD` showed both `main` and the
  alpha tag at `74194ab`.

Risk:

- Before any future push, replace the local placeholder author identity if the
  repository requires a personal or organization-controlled identity.
