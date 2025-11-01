#!/bin/bash

# FlatBuffers 构建脚本
# 用于生成 FlatBuffers 的 Rust 和 Java 代码

set -e

# 获取脚本目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

echo "=== FlatBuffers 代码生成 ==="

# 检查 flatc 是否安装
if ! command -v flatc &> /dev/null; then
    echo "Error: flatc (FlatBuffers compiler) not found!"
    echo "Please install FlatBuffers:"
    echo "  - Windows: Download from https://github.com/google/flatbuffers/releases"
    echo "  - macOS: brew install flatbuffers"
    echo "  - Linux: apt-get install flatbuffers-compiler"
    exit 1
fi

# 创建输出目录
mkdir -p src/generated
mkdir -p java/generated

# 生成 Rust 代码
echo "Generating Rust code..."
flatc --rust -o src/generated schemas/flux_cache.fbs

# 生成 Java 代码
echo "Generating Java code..."
flatc --java -o java/generated schemas/flux_cache.fbs

echo "FlatBuffers code generation completed!"
echo "Generated files:"
echo "  - src/generated/flux_cache_generated.rs"
echo "  - java/generated/com/flux/cache/FluxCacheMessage.java"
echo "  - java/generated/com/flux/cache/FluxCacheMessageBuilder.java"
echo "  - And other generated files..."

echo ""
echo "Next steps:"
echo "1. Update Rust code to use generated types"
echo "2. Update Java code to use generated classes"
echo "3. Build and test the application"
