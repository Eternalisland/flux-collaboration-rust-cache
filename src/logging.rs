use chrono::Local;
use fern::Dispatch;
use log::LevelFilter;
use std::{error::Error, fs, path::Path};
use std::sync::Once;

static INIT_LOGGER: Once = Once::new();

/// 只会真正初始化一次，多次调用是安全的。
///
/// `log_dir`: 日志目录，比如 "/var/log/flux-cache" 或 "./logs"
/// `level`: 全局日志级别，比如 LevelFilter::Info
pub fn init_logger_once(
    log_dir: Option<&str>,
    level: LevelFilter,
) -> Result<(), Box<dyn Error>> {
    let mut result: Result<(), Box<dyn Error>> = Ok(());

    INIT_LOGGER.call_once(|| {
        // 1. 处理日志目录
        let log_dir = log_dir.unwrap_or("logs");
        if let Err(e) = fs::create_dir_all(log_dir) {
            result = Err(Box::new(e));
            return;
        }

        // 2. 日志文件名：logs/native-cache-YYYY-MM-DD.log
        let date_str = Local::now().format("%Y-%m-%d").to_string();
        let log_file_path = format!("{}/rust-cache-{}.log", log_dir, date_str);
        
        // 3. 组装 fern
        let dispatch_result = Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "[{}][{}][{}] {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.target(),
                    message,
                ))
            })
            .level(level) // 全局最小级别
            // 控制台
            // .chain(std::io::stdout())
            // 文件
            .chain(
                fern::log_file(&log_file_path)
                    .map_err(|e| -> Box<dyn Error> { Box::new(e) }).unwrap(),
            )
            .apply();

        if let Err(e) = dispatch_result {
            // 如果已经有其他地方初始化过 logger，会进这里
            result = Err(Box::new(e));
        }
    });

    result
}
