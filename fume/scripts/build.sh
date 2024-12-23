#!/bin/bash
set -e

#!/bin/bash
script_dir=$(dirname "$(realpath "$BASH_SOURCE")")
fume_dir="$(dirname "$script_dir")"
repo_dir="$(dirname "$fume_dir")"
echo "fume_dir: $fume_dir"
echo "repo_dir: $repo_dir"
proto_path="$repo_dir/yellowstone-api/proto"
out_dir="$fume_dir/yellowstone_api"
module_name="yellowstone_api"
rm -fr $out_dir/*
mkdir -p $out_dir

/bin/env python -m grpc_tools.protoc -I$proto_path --python_out=$out_dir --pyi_out=$out_dir --grpc_python_out=$out_dir $proto_path/*.proto

pushd $out_dir
for file in *.py*; do
    name="${file%.*}"
    sed -i "s/import \(.*\)_pb2 as \(.*\)/import $module_name.\1_pb2 as \2/g" $file
    sed -i "s/import \(.*\)_pb2_grpc as \(.*\)/import $module_name.\1_pb2 as \2/g" $file
    sed -i "s/from \(.*\)_pb2_grpc import \(.*\)/from $module_name.\1_pb2 import \2/g" $file
    sed -i "s/from \(.*\)_pb2 import \(.*\)/from $module_name.\1_pb2 import \2/g" $file
done

touch '__init__.py'

popd