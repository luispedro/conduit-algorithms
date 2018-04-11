#!/usr/bin/env bash
set -e

shopt -s nullglob
exec stack bench --benchmark-arguments "--output bench.html"
