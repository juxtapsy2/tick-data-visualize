#!/bin/sh
set -e

PROTO_DIR="../../shared/proto"
OUT_DIR="./src/proto"

# Create output directory if it doesn't exist
mkdir -p $OUT_DIR

# Generate JavaScript code
protoc \
  --js_out=import_style=commonjs:$OUT_DIR \
  --grpc-web_out=import_style=typescript,mode=grpcwebtext:$OUT_DIR \
  --proto_path=$PROTO_DIR \
  $PROTO_DIR/market.proto

echo "Proto files generated successfully in $OUT_DIR"
