#!/bin/bash

if [ ! -f "`which cargo-watch`" ]; then
  if [ -f "`which cargo-binstall`" ]; then
    cargo binstall cargo-watch
  else
    echo Please accept the installation of cargo-watch next
    echo
    cargo install cargo-watch
  fi
fi
echo "run -- $@"
cargo watch -x "run -- $@"
