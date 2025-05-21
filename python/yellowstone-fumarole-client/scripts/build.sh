#!/bin/bash
set -e

script_dir=$(dirname "$(realpath "$BASH_SOURCE")")

# Go to parent directory as long as it is the not repo root
repo_dir="$script_dir"

while [ "$repo_dir" != "/" ] && [ ! -f "$repo_dir/Cargo.toml" ]; do
    repo_dir=$(dirname "$repo_dir")
done

if [ -f "$repo_dir/Cargo.toml" ]; then
    cd "$current_dir"
else
    echo "Cargo.toml not found in any parent directory."
    exit 1
fi


package_dir="$(dirname "$script_dir")"
echo "fume_dir: $package_dir"
echo "repo_dir: $repo_dir"
proto_path="$repo_dir/proto"
proto_path2="$repo_dir/yellowstone-grpc/yellowstone-grpc-proto/proto"
out_dir="$package_dir/yellowstone_api"
module_name="yellowstone_api"
rm -fr $out_dir/*
mkdir -p $out_dir

/bin/env python -m grpc_tools.protoc \
    -I$proto_path \
    -I$proto_path2 \
    --python_out=$out_dir \
    --pyi_out=$out_dir \
    --grpc_python_out=$out_dir \
    $proto_path/*.proto $proto_path2/*.proto

pushd $out_dir
for file in *.py*; do
    name="${file%.*}"
    sed -i "s/^import \(.*\)_pb2 as \(.*\)/import $module_name.\1_pb2 as \2/g" $file
    sed -i "s/^import \(.*\)_pb2_grpc as \(.*\)/import $module_name.\1_pb2 as \2/g" $file
    sed -i "s/^from \(.*\)_pb2_grpc import \(.*\)/from $module_name.\1_pb2 import \2/g" $file
    sed -i "s/^from \(.*\)_pb2 import \(.*\)/from $module_name.\1_pb2 import \2/g" $file
done

touch '__init__.py'

popd