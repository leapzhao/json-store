package router

import (
	"github.com/leapzhao/json-store/config"
	"github.com/leapzhao/json-store/database"
	"github.com/leapzhao/json-store/handler"
	"github.com/leapzhao/json-store/middleware"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// Init 初始化路由
func Init(cfg config.Config, store database.JSONStore) *gin.Engine {
	// 设置Gin模式
	setGinMode(cfg.Environment)

	// 创建Gin引擎
	router := gin.New()

	// 添加中间件
	router.Use(middleware.Recovery())
	router.Use(middleware.RequestLogger())
	router.Use(middleware.RequestID())

	// 添加CORS中间件
	router.Use(cors.New(cors.Config{
		AllowOrigins:     cfg.Security.CorsOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// 创建处理器
	jsonHandler := handler.NewJSONHandler(store)

	// 注册路由
	registerRoutes(router, jsonHandler, cfg)

	log.Info().Msg("Router initialized")

	return router
}

// setGinMode 根据环境设置Gin模式
func setGinMode(env config.Environment) {
	switch env {
	case config.EnvProduct:
		gin.SetMode(gin.ReleaseMode)
	case config.EnvTest:
		gin.SetMode(gin.TestMode)
	default: // local
		gin.SetMode(gin.DebugMode)
	}
}

// registerRoutes 注册路由
func registerRoutes(router *gin.Engine, handler *handler.JSONHandler, cfg config.Config) {
	// 健康检查
	router.GET("/health", handler.HealthCheck)
	router.GET("/ready", handler.ReadyCheck)
	router.GET("/version", handler.Version)

	// API路由组
	api := router.Group("/api")
	{
		// API版本控制
		v1 := api.Group("/v1")
		{
			v1.POST("/json", handler.StoreJSON)
			v1.GET("/json/:id", handler.GetJSON)
			v1.GET("/json", handler.GetJSONByHash)

			// 批量操作
			v1.POST("/json/batch", handler.StoreJSONBatch)
			v1.GET("/json/batch", handler.GetJSONBatch)
		}

		// 管理接口（生产环境需要认证）
		if cfg.Environment == config.EnvProduct {
			admin := api.Group("/admin")
			admin.Use(middleware.BasicAuth())
			{
				admin.GET("/metrics", handler.Metrics)
				admin.GET("/stats", handler.Stats)
			}
		}
	}

	// 404处理
	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{
			"error":   "NOT_FOUND",
			"message": "The requested resource was not found",
		})
	})
}
