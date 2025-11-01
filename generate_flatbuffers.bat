@echo off
setlocal

REM FlatBuffers 构建脚本 (Windows)
REM 用于生成 FlatBuffers 的 Rust 和 Java 代码

REM 获取脚本目录
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%"

echo === FlatBuffers 代码生成 ===

REM 检查 flatc 是否安装
where flatc >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: flatc (FlatBuffers compiler) not found!
    echo Please install FlatBuffers:
    echo   - Download from https://github.com/google/flatbuffers/releases
    echo   - Extract and add to PATH
    exit /b 1
)

REM 创建输出目录
if not exist "src\generated" mkdir "src\generated"
if not exist "java\generated" mkdir "java\generated"

REM 生成 Rust 代码
echo Generating Rust code...
flatc --rust -o src\generated schemas\flux_cache.fbs

REM 生成 Java 代码
echo Generating Java code...
flatc --java -o java\generated schemas\flux_cache.fbs

echo FlatBuffers code generation completed!
echo Generated files:
echo   - src\generated\flux_cache_generated.rs
echo   - java\generated\com\flux\cache\FluxCacheMessage.java
echo   - java\generated\com\flux\cache\FluxCacheMessageBuilder.java
echo   - And other generated files...

echo.
echo Next steps:
echo 1. Update Rust code to use generated types
echo 2. Update Java code to use generated classes
echo 3. Build and test the application

popd
endlocal
