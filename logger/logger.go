package logger

import (
	"fmt"
	"github.com/leapzhao/json-store/config"
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	globalLogger zerolog.Logger
	initialized  bool
)

// Init 初始化日志系统
func Init(cfg config.Config) error {
	// 设置日志级别
	level, err := zerolog.ParseLevel(cfg.Logging.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// 设置时间格式
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// 创建日志输出
	var output io.Writer
	switch cfg.Logging.Format {
	case "json":
		output = os.Stdout
		if cfg.Logging.OutputPath != "stdout" {
			output = createLogFile(cfg.Logging.OutputPath, cfg.Environment)
		}
	default: // console
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}
		if cfg.Logging.OutputPath != "stdout" {
			output = createLogFile(cfg.Logging.OutputPath, cfg.Environment)
		}
	}

	// 构建logger
	globalLogger = zerolog.New(output).
		With().
		Timestamp().
		Str("app", "json-store").
		Str("env", string(cfg.Environment)).
		Logger()

	// 设置全局logger
	log.Logger = globalLogger

	initialized = true

	log.Info().
		Str("level", cfg.Logging.Level).
		Str("format", cfg.Logging.Format).
		Str("output", cfg.Logging.OutputPath).
		Msg("Logger initialized")

	return nil
}

// GetLogger 获取全局logger
func GetLogger() zerolog.Logger {
	if !initialized {
		// 返回默认logger
		return zerolog.New(os.Stdout).With().Timestamp().Logger()
	}
	return globalLogger
}

// createLogFile 创建日志文件输出
func createLogFile(path string, env config.Environment) io.Writer {
	// 根据环境添加后缀
	filename := path
	if env == config.EnvProduct {
		filename = fmt.Sprintf("%s.%s.log", path, env)
	}

	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    100, // MB
		MaxBackups: 10,
		MaxAge:     30, // days
		Compress:   true,
	}
}

// WithContext 创建带有请求ID的logger
func WithContext(requestID string) zerolog.Logger {
	return globalLogger.With().Str("request_id", requestID).Logger()
}
