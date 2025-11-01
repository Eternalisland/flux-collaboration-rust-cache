@echo off
REM build.bat - Windows 构建 JNI 动态库脚本

echo === 构建 Flux 缓存 JNI 动态库 ===

REM 检查 Rust 是否安装
where cargo >nul 2>nul
if %errorlevel% neq 0 (
    echo 错误: 未找到 cargo，请先安装 Rust
    exit /b 1
)

REM 检查 Java 是否安装
where javac >nul 2>nul
if %errorlevel% neq 0 (
    echo 错误: 未找到 javac，请先安装 Java JDK
    exit /b 1
)

REM 设置环境变量
if "%JAVA_HOME%"=="" (
    for /f "tokens=*" %%i in ('where javac') do (
        set JAVA_HOME=%%~dpi..\..
    )
)

echo JAVA_HOME: %JAVA_HOME%

REM 创建必要的目录
if not exist "target\release" mkdir target\release
if not exist "java\classes" mkdir java\classes

REM 编译 Java 类
echo 编译 Java 类...
javac -d java\classes java\*.java
if %errorlevel% neq 0 (
    echo Java 编译失败
    exit /b 1
)

REM 构建 Rust 动态库
echo 构建 Rust 动态库...
cargo build --release
if %errorlevel% neq 0 (
    echo Rust 构建失败
    exit /b 1
)

REM 复制动态库到目标目录
echo 复制动态库...
copy target\release\flux_collaboration_rust_cache.dll target\release\
if %errorlevel% neq 0 (
    echo 复制动态库失败
    exit /b 1
)

echo Windows 动态库构建完成: target\release\flux_collaboration_rust_cache.dll

echo === 构建完成 ===
echo.
echo 使用方法:
echo 1. 将 target\release 目录添加到 PATH 环境变量
echo 2. 运行 Java 程序:
echo    java -cp java\classes com.flux.collaboration.cache.FluxCacheExample
echo.
echo 动态库位置: target\release\
pause
