#!/usr/bin/env bash
# scripts/generate_proto.sh
# Regenerate Python protobuf bindings from .proto source files.
#
# Prerequisites:
#   pip install grpcio-tools
#
# Usage:
#   chmod +x scripts/generate_proto.sh
#   ./scripts/generate_proto.sh

set -euo pipefail

PROTO_DIR="kafka_mongo_pipeline/proto"
OUT_DIR="kafka_mongo_pipeline/proto"

echo "Generating Python protobuf bindings..."

python -m grpc_tools.protoc \
  --proto_path="${PROTO_DIR}" \
  --python_out="${OUT_DIR}" \
  "${PROTO_DIR}/customer_event.proto"

echo "Done. Generated files in ${OUT_DIR}/"
echo ""
echo "Generated:"
ls -1 "${OUT_DIR}"/*_pb2.py
