package middleware

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/leapzhao/json-store/logger"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// RequestLogger 请求日志中间件
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// 读取请求体（用于日志）
		var requestBody []byte
		if c.Request.Body != nil {
			requestBody, _ = io.ReadAll(c.Request.Body)
			c.Request.Body = io.NopCloser(bytes.NewBuffer(requestBody))
		}

		// 处理请求
		c.Next()

		// 获取请求ID
		requestID := c.GetString("request_id")
		if requestID == "" {
			requestID = "unknown"
		}

		// 记录日志
		log := logger.WithContext(requestID)
		log.Info().
			Str("method", c.Request.Method).
			Str("path", path).
			Str("query", query).
			Int("status", c.Writer.Status()).
			Int("body_size", c.Writer.Size()).
			Dur("latency", time.Since(start)).
			Str("client_ip", c.ClientIP()).
			Str("user_agent", c.Request.UserAgent()).
			Interface("errors", c.Errors.Errors()).
			Msg("HTTP Request")

		// 记录慢请求
		latency := time.Since(start)
		if latency > time.Second {
			log.Warn().
				Dur("latency", latency).
				Str("path", path).
				Msg("Slow request detected")
		}
	}
}

// RequestID 请求ID中间件
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 尝试从请求头获取请求ID
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			// 生成新的请求ID
			requestID = uuid.New().String()
		}

		// 设置到上下文
		c.Set("request_id", requestID)

		// 设置响应头
		c.Header("X-Request-ID", requestID)

		c.Next()
	}
}

// Recovery 恢复中间件
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				requestID := c.GetString("request_id")
				log := logger.WithContext(requestID)

				// 记录panic信息
				log.Error().
					Interface("error", err).
					Str("path", c.Request.URL.Path).
					Str("method", c.Request.Method).
					Msg("Panic recovered")

				// 返回错误响应
				c.JSON(500, gin.H{
					"error":      "INTERNAL_SERVER_ERROR",
					"message":    "An unexpected error occurred",
					"request_id": requestID,
				})

				c.Abort()
			}
		}()

		c.Next()
	}
}

// BodySizeLimit 请求体大小限制中间件
func BodySizeLimit(maxSize int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSize)
		c.Next()
	}
}

// ValidateJSON JSON验证中间件
func ValidateJSON() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "POST" || c.Request.Method == "PUT" {
			var body map[string]interface{}

			// 检查Content-Type
			contentType := c.GetHeader("Content-Type")
			if contentType != "application/json" {
				c.JSON(400, gin.H{
					"error":   "INVALID_CONTENT_TYPE",
					"message": "Content-Type must be application/json",
				})
				c.Abort()
				return
			}

			// 验证JSON
			if err := c.ShouldBindJSON(&body); err != nil {
				c.JSON(400, gin.H{
					"error":   "INVALID_JSON",
					"message": "Invalid JSON format",
				})
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

// BasicAuth 基本认证中间件
func BasicAuth() gin.HandlerFunc {
	return gin.BasicAuth(gin.Accounts{
		"admin": "secret", // 实际应用中应该从配置读取
	})
}

// RateLimit 限流中间件
func RateLimit(limit int) gin.HandlerFunc {
	limiter := make(chan struct{}, limit)

	return func(c *gin.Context) {
		select {
		case limiter <- struct{}{}:
			defer func() { <-limiter }()
			c.Next()
		default:
			c.JSON(429, gin.H{
				"error":   "TOO_MANY_REQUESTS",
				"message": "Rate limit exceeded",
			})
			c.Abort()
		}
	}
}
