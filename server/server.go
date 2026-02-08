package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/leapzhao/json-store/config"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type Server struct {
	httpServer *http.Server
	config     config.Config
	router     *gin.Engine
}

// New 创建HTTP服务器
func New(cfg config.Config, router *gin.Engine) *Server {
	return &Server{
		config: cfg,
		router: router,
	}
}

// Start 启动HTTP服务器
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%s", s.config.Server.Host, s.config.Server.Port)

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  time.Duration(s.config.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(s.config.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(s.config.Server.IdleTimeout) * time.Second,
	}

	log.Info().
		Str("address", addr).
		Str("environment", string(s.config.Environment)).
		Msg("Starting HTTP server")

	// 根据配置决定启动HTTP还是HTTPS
	if s.config.Security.EnableHTTPS {
		return s.startHTTPS()
	}

	return s.startHTTP()
}

// startHTTP 启动HTTP服务器
func (s *Server) startHTTP() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	return nil
}

// startHTTPS 启动HTTPS服务器
func (s *Server) startHTTPS() error {
	if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" {
		return fmt.Errorf("certificate and key files are required for HTTPS")
	}

	if err := s.httpServer.ListenAndServeTLS(
		s.config.Security.CertFile,
		s.config.Security.KeyFile,
	); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTPS server: %w", err)
	}
	return nil
}

// Shutdown 优雅关闭服务器
func (s *Server) Shutdown(ctx context.Context) error {
	log.Info().Msg("Shutting down HTTP server...")

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server forced to shutdown: %w", err)
	}

	log.Info().Msg("HTTP server shutdown completed")
	return nil
}

// GetHTTPServer 获取HTTP服务器实例
func (s *Server) GetHTTPServer() *http.Server {
	return s.httpServer
}
