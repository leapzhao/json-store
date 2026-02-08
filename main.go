package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leapzhao/json-store/config"
	"github.com/leapzhao/json-store/database"
	"github.com/leapzhao/json-store/handler"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// 初始化日志
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// 加载配置
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// 创建数据库存储（工厂模式）
	store, err := database.CreateStore(*cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create database store")
	}
	defer store.Close()

	// 初始化Gin
	if cfg.Server.Port == "" {
		cfg.Server.Port = "8080"
	}

	// 根据环境设置Gin模式
	if os.Getenv("GIN_MODE") == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	// 中间件
	router.Use(gin.Recovery())
	router.Use(loggerMiddleware())

	// 创建处理器
	jsonHandler := handler.NewJSONHandler(store)

	// 路由
	v1 := router.Group("/api/v1")
	{
		v1.POST("/json", jsonHandler.StoreJSON)
		v1.GET("/json/:id", jsonHandler.GetJSON)
		v1.GET("/json", jsonHandler.GetJSONByHash)
		v1.GET("/health", jsonHandler.HealthCheck)
	}

	// 启动服务器
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 优雅关闭
	go func() {
		log.Info().Str("port", cfg.Server.Port).Msg("Starting JSON store server")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Failed to start server")
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("Server forced to shutdown")
	}

	log.Info().Msg("Server exited properly")
}

// 日志中间件
func loggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		// 处理请求
		c.Next()

		// 记录日志
		log.Info().
			Str("method", c.Request.Method).
			Str("path", path).
			Int("status", c.Writer.Status()).
			Dur("duration", time.Since(start)).
			Str("client_ip", c.ClientIP()).
			Msg("HTTP request")
	}
}
