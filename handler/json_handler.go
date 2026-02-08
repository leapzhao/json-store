package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/leapzhao/json-store/database"
	"github.com/leapzhao/json-store/model"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
)

type JSONHandler struct {
	store      database.JSONStore
	appVersion string
	buildTime  string
	gitCommit  string
	startTime  time.Time
}

func NewJSONHandler(store database.JSONStore) *JSONHandler {
	return &JSONHandler{
		store:      store,
		appVersion: "1.0.0",
		buildTime:  time.Now().Format(time.RFC3339),
		gitCommit:  "unknown",
		startTime:  time.Now(),
	}
}

// StoreJSON 存储JSON
func (h *JSONHandler) StoreJSON(c *gin.Context) {
	var req model.StoreRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "INVALID_REQUEST",
			Message: "Invalid request body",
		})
		return
	}

	// 验证JSON数据
	validate := validator.New()
	if err := validate.Struct(req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "VALIDATION_ERROR",
			Message: err.Error(),
		})
		return
	}

	// 存储JSON
	start := time.Now()
	doc, err := h.store.StoreJSON(c.Request.Context(), req.JSONData)
	if err != nil {
		log.Error().Err(err).Msg("Failed to store JSON")
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error:   "STORAGE_ERROR",
			Message: "Failed to store JSON document",
		})
		return
	}

	// 检查是否是新建
	isNew := time.Since(doc.CreatedAt) < time.Second

	response := model.StoreResponse{
		ID:        doc.ID,
		IsNew:     isNew,
		CreatedAt: doc.CreatedAt,
		Message:   getStorageMessage(isNew),
	}

	log.Info().
		Str("id", doc.ID).
		Bool("is_new", isNew).
		Dur("duration", time.Since(start)).
		Msg("JSON stored successfully")

	c.JSON(http.StatusOK, response)
}

// StoreJSONBatch 批量存储JSON
func (h *JSONHandler) StoreJSONBatch(c *gin.Context) {
	var req model.StoreBatchRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "INVALID_REQUEST",
			Message: "Invalid request body",
		})
		return
	}

	// 验证请求
	validate := validator.New()
	if err := validate.Struct(req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "VALIDATION_ERROR",
			Message: err.Error(),
		})
		return
	}

	// 提取JSON数据
	start := time.Now()
	jsonDataList := make([][]byte, 0, len(req.Documents))

	for i, docReq := range req.Documents {
		// 验证每个文档的JSON
		if !json.Valid(docReq.JSONData) {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{
				Error:   "INVALID_JSON",
				Message: fmt.Sprintf("Document at index %d contains invalid JSON", i),
			})
			return
		}
		jsonDataList = append(jsonDataList, docReq.JSONData)
	}

	// 批量存储
	results, err := h.store.StoreJSONBatch(c.Request.Context(), jsonDataList)
	if err != nil {
		log.Error().Err(err).Msg("Failed to store JSON batch")
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error:   "BATCH_STORAGE_ERROR",
			Message: "Failed to store JSON documents in batch",
		})
		return
	}

	// 构建响应
	response := model.StoreBatchResponse{
		TotalCount:   len(req.Documents),
		SuccessCount: len(results),
		FailureCount: len(req.Documents) - len(results),
		Duration:     time.Since(start),
		Results:      make([]model.StoreResponse, 0, len(results)),
	}

	for _, doc := range results {
		isNew := time.Since(doc.CreatedAt) < time.Second
		response.Results = append(response.Results, model.StoreResponse{
			ID:        doc.ID,
			IsNew:     isNew,
			CreatedAt: doc.CreatedAt,
			Message:   getStorageMessage(isNew),
		})
	}

	// 如果有失败，添加失败信息
	if response.FailureCount > 0 {
		response.Failures = []model.BatchFailure{
			{
				Index:   response.SuccessCount,
				Error:   "PROCESSING_ERROR",
				Message: "Some documents failed to process",
			},
		}
	}

	log.Info().
		Int("total", response.TotalCount).
		Int("success", response.SuccessCount).
		Int("failure", response.FailureCount).
		Dur("duration", response.Duration).
		Msg("JSON batch stored successfully")

	c.JSON(http.StatusOK, response)
}

// GetJSON 根据ID获取JSON
func (h *JSONHandler) GetJSON(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "MISSING_ID",
			Message: "Document ID is required",
		})
		return
	}

	doc, err := h.store.GetJSONByID(c.Request.Context(), id)
	if err != nil {
		log.Error().Err(err).Str("id", id).Msg("Failed to get JSON")
		c.JSON(http.StatusNotFound, model.ErrorResponse{
			Error:   "NOT_FOUND",
			Message: "Document not found",
		})
		return
	}

	c.JSON(http.StatusOK, doc)
}

// GetJSONBatch 批量获取JSON
func (h *JSONHandler) GetJSONBatch(c *gin.Context) {
	var req model.GetBatchRequest

	// 尝试从URL参数获取
	idsParam := c.Query("ids")
	if idsParam != "" {
		// 从逗号分隔的字符串解析ID
		ids := strings.Split(idsParam, ",")
		if len(ids) > 0 {
			req.IDs = ids
		}
	}

	// 如果没有URL参数，尝试从请求体获取
	if len(req.IDs) == 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, model.ErrorResponse{
				Error:   "INVALID_REQUEST",
				Message: "Invalid request body or missing IDs",
			})
			return
		}
	}

	// 验证请求
	if len(req.IDs) == 0 {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "MISSING_IDS",
			Message: "At least one ID is required",
		})
		return
	}

	if len(req.IDs) > 100 {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "TOO_MANY_IDS",
			Message: "Maximum 100 IDs allowed per request",
		})
		return
	}

	validate := validator.New()
	if err := validate.Struct(req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "VALIDATION_ERROR",
			Message: err.Error(),
		})
		return
	}

	// 批量获取
	start := time.Now()
	documents, err := h.store.GetJSONBatch(c.Request.Context(), req.IDs)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get JSON batch")
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error:   "BATCH_GET_ERROR",
			Message: "Failed to get JSON documents in batch",
		})
		return
	}

	// 构建响应
	response := model.GetBatchResponse{
		SuccessCount: len(documents),
		FailureCount: len(req.IDs) - len(documents),
		Documents:    make([]model.JSONDocument, 0, len(documents)),
	}

	for _, doc := range documents {
		response.Documents = append(response.Documents, *doc)
	}

	// 如果有失败，识别哪些ID没找到
	if response.FailureCount > 0 {
		foundIDs := make(map[string]bool)
		for _, doc := range documents {
			foundIDs[doc.ID] = true
		}

		for i, id := range req.IDs {
			if !foundIDs[id] {
				response.Failures = append(response.Failures, model.BatchFailure{
					Index:   i,
					Error:   "NOT_FOUND",
					Message: fmt.Sprintf("Document with ID %s not found", id),
				})
			}
		}
	}

	log.Info().
		Int("requested", len(req.IDs)).
		Int("found", response.SuccessCount).
		Dur("duration", time.Since(start)).
		Msg("JSON batch retrieved successfully")

	c.JSON(http.StatusOK, response)
}

// GetJSONByHash 根据哈希值获取JSON
func (h *JSONHandler) GetJSONByHash(c *gin.Context) {
	hash := c.Query("hash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{
			Error:   "MISSING_HASH",
			Message: "Hash parameter is required",
		})
		return
	}

	doc, err := h.store.GetJSONByHash(c.Request.Context(), hash)
	if err != nil {
		c.JSON(http.StatusNotFound, model.ErrorResponse{
			Error:   "NOT_FOUND",
			Message: "Document not found with the provided hash",
		})
		return
	}

	c.JSON(http.StatusOK, doc)
}

// HealthCheck 健康检查
func (h *JSONHandler) HealthCheck(c *gin.Context) {
	status := "healthy"

	// 检查数据库连接
	var dbStatus bool
	if err := h.store.HealthCheck(c.Request.Context()); err != nil {
		status = "unhealthy"
		dbStatus = false
		log.Error().Err(err).Msg("Database health check failed")
	} else {
		dbStatus = true
	}

	response := model.HealthResponse{
		Status:    status,
		Timestamp: time.Now(),
		Database:  dbStatus,
		Version:   h.appVersion,
	}

	statusCode := http.StatusOK
	if status == "unhealthy" {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, response)
}

// ReadyCheck 就绪检查
func (h *JSONHandler) ReadyCheck(c *gin.Context) {
	ready := true
	checks := []model.HealthCheck{}

	// 检查数据库连接
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.store.HealthCheck(ctx); err != nil {
		ready = false
		checks = append(checks, model.HealthCheck{
			Name:   "database",
			Status: "failed",
			Error:  err.Error(),
		})
	} else {
		checks = append(checks, model.HealthCheck{
			Name:   "database",
			Status: "ok",
		})
	}

	response := model.ReadyResponse{
		Ready:     ready,
		Timestamp: time.Now(),
		Checks:    checks,
	}

	statusCode := http.StatusOK
	if !ready {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, response)
}

// Version 版本信息
func (h *JSONHandler) Version(c *gin.Context) {
	response := model.VersionResponse{
		Version:     h.appVersion,
		BuildTime:   h.buildTime,
		GitCommit:   h.gitCommit,
		Environment: GetEnvironment(),
		GoVersion:   runtime.Version(),
	}

	c.JSON(http.StatusOK, response)
}

// Metrics 获取性能指标
func (h *JSONHandler) Metrics(c *gin.Context) {
	// 检查认证
	user, password, hasAuth := c.Request.BasicAuth()
	if hasAuth {
		if user != "admin" || password != "secret" {
			c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
			c.JSON(http.StatusUnauthorized, model.ErrorResponse{
				Error:   "UNAUTHORIZED",
				Message: "Authentication required",
			})
			return
		}
	} else {
		c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
		c.JSON(http.StatusUnauthorized, model.ErrorResponse{
			Error:   "UNAUTHORIZED",
			Message: "Authentication required",
		})
		return
	}

	// 获取数据库指标
	metrics, err := h.store.GetMetrics(c.Request.Context())
	if err != nil {
		log.Error().Err(err).Msg("Failed to get metrics")
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error:   "METRICS_ERROR",
			Message: "Failed to retrieve metrics",
		})
		return
	}

	// 添加应用指标
	metrics.Uptime = time.Since(h.startTime)

	c.JSON(http.StatusOK, metrics)
}

// Stats 获取统计信息
func (h *JSONHandler) Stats(c *gin.Context) {
	// 检查认证
	user, password, hasAuth := c.Request.BasicAuth()
	if hasAuth {
		if user != "admin" || password != "secret" {
			c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
			c.JSON(http.StatusUnauthorized, model.ErrorResponse{
				Error:   "UNAUTHORIZED",
				Message: "Authentication required",
			})
			return
		}
	} else {
		c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
		c.JSON(http.StatusUnauthorized, model.ErrorResponse{
			Error:   "UNAUTHORIZED",
			Message: "Authentication required",
		})
		return
	}

	// 获取统计信息
	stats, err := h.store.GetStats(c.Request.Context())
	if err != nil {
		log.Error().Err(err).Msg("Failed to get stats")
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{
			Error:   "STATS_ERROR",
			Message: "Failed to retrieve statistics",
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

func getStorageMessage(isNew bool) string {
	if isNew {
		return "JSON document stored successfully"
	}
	return "JSON document already exists, returning existing ID"
}

// GetEnvironment 获取环境信息
func GetEnvironment() string {
	env := os.Getenv("APP_ENV")
	if env == "" {
		return "development"
	}
	return env
}
