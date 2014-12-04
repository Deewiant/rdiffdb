#!/bin/zsh
set -o errexit
clang++ -c -O3 -flto -march=native marisa.cc -std=c++14 -pedantic -Weverything
clang -ordiffdb rdiffdb.c marisa.o -lleveldb -lmarisa -lstdc++ $(pkg-config --cflags --libs glib-2.0) -std=c11 -pedantic -Weverything -Wno-{cast-align,disabled-macro-expansion,documentation{,-unknown-command},sign-{conversion,compare}} -O3 -flto -march=native -fuse-ld=gold
