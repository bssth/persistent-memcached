#!/usr/bin/env bash
# Prints the cargo target directory of this repo, as scripts/cargo.sh builds it: it honours
# CARGO_TARGET_DIR, cargo config, and the WSL rule in cargo.sh. Scripts that run the pmcd
# binary use this so they look where the build put it.
set -euo pipefail
bash "$(dirname "$0")/cargo.sh" metadata --no-deps --format-version 1 \
  | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
