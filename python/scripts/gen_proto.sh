#!/bin/sh
# Regenerate the Python gRPC code in src/tidelet/_proto from api/tidelet/v1alpha1/tidelet.proto.
# Needs grpcio-tools and googleapis-common-protos in the Python that runs it (PYTHON, default python3).
set -eu
cd "$(dirname "$0")/.."
PY=${PYTHON:-python3}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
# The file is compiled under the path tidelet/_proto/, so the generated modules import each
# other as tidelet._proto.*; the protobuf package (tidelet.v1alpha1) is what the wire uses.
mkdir -p "$tmp/tidelet/_proto"
cp ../api/tidelet/v1alpha1/tidelet.proto "$tmp/tidelet/_proto/tidelet.proto"
site=$("$PY" -c 'import google.api, os; print(os.path.dirname(os.path.dirname(os.path.dirname(google.api.annotations_pb2.__file__))))' 2>/dev/null ||
       "$PY" -c 'import google.api.annotations_pb2 as a, os; print(os.path.dirname(os.path.dirname(os.path.dirname(a.__file__))))')
"$PY" -m grpc_tools.protoc -I "$tmp" -I "$site" --python_out=src --pyi_out=src --grpc_python_out=src "$tmp/tidelet/_proto/tidelet.proto"
echo "generated src/tidelet/_proto/tidelet_pb2.py, tidelet_pb2.pyi, tidelet_pb2_grpc.py"
