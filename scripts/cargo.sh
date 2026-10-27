#!/usr/bin/env bash
# Run cargo for this repo. When the sources live on a Windows drive mounted into WSL
# (/mnt/<drive>/...), keep the target dir on the Linux filesystem: it is much faster
# and the Windows drive may be short on space.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "$PWD" == /mnt/* && -z "${CARGO_TARGET_DIR:-}" ]]; then
  export CARGO_TARGET_DIR="$HOME/.cache/pmcd-target"
fi
[[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
exec cargo "$@"
