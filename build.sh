#!/bin/zsh
set -o errexit
clang -ordiffdb rdiffdb.c -lleveldb $(pkg-config --cflags --libs glib-2.0) -std=c11 -pedantic -Weverything -Wno-{cast-align,disabled-macro-expansion,documentation{,-unknown-command},sign-{conversion,compare}} -O3 -flto -march=native -fuse-ld=gold
