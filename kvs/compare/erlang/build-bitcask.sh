#!/bin/sh
# Builds basho/bitcask, for bench.escript, in $1 or ~/.cache/kvs-compare,
# and prints the ERL_LIBS to run the script with.
set -eu
dir=${1:-${XDG_CACHE_HOME:-$HOME/.cache}/kvs-compare}
mkdir -p "$dir"
cd "$dir"
if [ ! -x rebar3 ]; then
	curl -fsSLo rebar3 https://s3.amazonaws.com/rebar3/rebar3
	chmod +x rebar3
fi
[ -d bitcask ] || git clone -q --depth 1 https://github.com/basho/bitcask.git
cd bitcask
../rebar3 compile >&2
echo "ERL_LIBS=$PWD/_build/default/lib"
