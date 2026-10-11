#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
command -v pkg-config >/dev/null
pkg-config --exists luajit || {
  echo "LuaJIT development files (pkg-config module 'luajit') are required" >&2
  exit 1
}

output_dir="$(mktemp -d)"
trap 'rm -rf "$output_dir"' EXIT

c++ -std=c++20 -Wall -Wextra lua-poc.cc $(pkg-config --cflags --libs luajit) \
  -o "$output_dir/lua-poc"
"$output_dir/lua-poc"
printf 'Lua runtime seam prototype passed\n'
