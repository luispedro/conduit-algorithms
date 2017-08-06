#!/usr/bin/env bash

mkdir -p ~/.local/bin
export PATH=$HOME/.local/bin:$PATH

if test -e $HOME/.local/bin/stack; then
    echo "stack already installed"
else
    curl -L https://www.stackage.org/stack/linux-x86_64 | tar xz --wildcards --strip-components=1 -C ~/.local/bin '*/stack'
fi
