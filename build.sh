#!/bin/bash
# build.sh - 构建 JNI 动态库脚本

echo "=== 构建 Flux 缓存 JNI 动态库 ==="

# 检查 Rust 是否安装
if ! command -v cargo &> /dev/null; then
    echo "错误: 未找到 cargo，请先安装 Rust"
    exit 1
fi

# 检查 Java 是否安装
if ! command -v javac &> /dev/null; then
    echo "错误: 未找到 javac，请先安装 Java JDK"
    exit 1
fi

# 设置环境变量
export JAVA_HOME=${JAVA_HOME:-$(dirname $(dirname $(readlink -f $(which javac))))}
export LD_LIBRARY_PATH=$PWD/target/release:$LD_LIBRARY_PATH

echo "JAVA_HOME: $JAVA_HOME"

# 创建必要的目录
mkdir -p target/release
mkdir -p java/classes

# 编译 Java 类
echo "编译 Java 类..."
javac -d java/classes java/*.java
if [ $? -ne 0 ]; then
    echo "Java 编译失败"
    exit 1
fi

# 构建 Rust 动态库
echo "构建 Rust 动态库..."
cargo build --release
if [ $? -ne 0 ]; then
    echo "Rust 构建失败"
    exit 1
fi

# 复制动态库到目标目录
echo "复制动态库..."
case "$(uname -s)" in
    Linux*)
        cp target/release/libflux_collaboration_rust_cache.so target/release/
        echo "Linux 动态库构建完成: target/release/libflux_collaboration_rust_cache.so"
        ;;
    Darwin*)
        cp target/release/libflux_collaboration_rust_cache.dylib target/release/
        echo "macOS 动态库构建完成: target/release/libflux_collaboration_rust_cache.dylib"
        ;;
    MINGW*|CYGWIN*|MSYS*)
        cp target/release/flux_collaboration_rust_cache.dll target/release/
        echo "Windows 动态库构建完成: target/release/flux_collaboration_rust_cache.dll"
        ;;
    *)
        echo "未知的操作系统: $(uname -s)"
        exit 1
        ;;
esac

echo "=== 构建完成 ==="
echo ""
echo "使用方法:"
echo "1. 将动态库路径添加到 LD_LIBRARY_PATH (Linux/macOS) 或 PATH (Windows)"
echo "2. 运行 Java 程序:"
echo "   java -cp java/classes com.flux.collaboration.cache.FluxCacheExample"
echo ""
echo "动态库位置: target/release/"
