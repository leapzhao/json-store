package main

import (
	"github.com/leapzhao/json-store/app"
	"github.com/leapzhao/json-store/logger"
	"os"
	"runtime"

	"github.com/rs/zerolog/log"
)

// @title JSON Store API
// @version 1.0
// @description A service for storing and retrieving JSON documents
// @contact.name API Support
// @contact.email support@jsonstore.com
// @host localhost:8080
// @BasePath /api/v1
func main() {
	// 打印应用信息
	printAppInfo()

	// 创建应用
	application, err := app.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create application")
	}

	// 运行应用
	if err := application.Run(); err != nil {
		log.Fatal().Err(err).Msg("Application failed")
	}

	log.Info().Msg("Application exited successfully")
}

// printAppInfo 打印应用信息
func printAppInfo() {
	appName := os.Getenv("APP_NAME")
	if appName == "" {
		appName = "JSON Store"
	}

	appVersion := os.Getenv("APP_VERSION")
	if appVersion == "" {
		appVersion = "1.0.0"
	}

	log.Info().
		Str("app_name", appName).
		Str("version", appVersion).
		Str("go_version", runtime.Version()).
		Int("num_cpu", runtime.NumCPU()).
		Msg("Starting application")
}

// init 初始化函数
func init() {
	// 设置GOMAXPROCS
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 确保有全局logger
	logger.GetLogger()
}
