package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leapzhao/json-store/config"
	"github.com/leapzhao/json-store/database"
	"github.com/leapzhao/json-store/logger"
	"github.com/leapzhao/json-store/router"
	"github.com/leapzhao/json-store/server"

	"github.com/rs/zerolog/log"
)

type Application struct {
	config *config.Config
	store  database.JSONStore
	server *server.Server
}

// New 创建应用实例
func New() (*Application, error) {
	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 初始化日志
	if err := logger.Init(*cfg); err != nil {
		return nil, fmt.Errorf("failed to init logger: %w", err)
	}

	// 创建数据库存储
	store, err := database.CreateStore(*cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create database store: %w", err)
	}

	// 健康检查数据库连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.HealthCheck(ctx); err != nil {
		return nil, fmt.Errorf("database health check failed: %w", err)
	}

	log.Info().
		Str("database_type", cfg.Database.Type).
		Str("database_host", cfg.Database.Host).
		Msg("Database connection established")

	return &Application{
		config: cfg,
		store:  store,
	}, nil
}

// Start 启动应用
func (app *Application) Start() error {
	// 初始化路由
	ginRouter := router.Init(*app.config, app.store)

	// 创建HTTP服务器
	app.server = server.New(*app.config, ginRouter)

	// 启动服务器
	go func() {
		if err := app.server.Start(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start server")
		}
	}()

	return nil
}

// Shutdown 关闭应用
func (app *Application) Shutdown() error {
	// 关闭数据库连接
	if err := app.store.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close database connection")
	}

	log.Info().Msg("Application shutdown completed")
	return nil
}

// Run 运行应用
func (app *Application) Run() error {
	// 启动应用
	if err := app.Start(); err != nil {
		return err
	}

	// 等待中断信号
	app.waitForShutdown()

	return nil
}

// waitForShutdown 等待关闭信号
func (app *Application) waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-quit
	log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")

	// 创建关闭上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 关闭服务器
	if err := app.server.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("Server shutdown error")
	}

	// 关闭应用
	if err := app.Shutdown(); err != nil {
		log.Error().Err(err).Msg("Application shutdown error")
	}
}

// GetConfig 获取配置
func (app *Application) GetConfig() *config.Config {
	return app.config
}

// GetStore 获取数据库存储
func (app *Application) GetStore() database.JSONStore {
	return app.store
}
