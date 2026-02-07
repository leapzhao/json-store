package handler

import (
	"github.com/leapzhao/json-store/database"
	"github.com/leapzhao/json-store/model"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
)

type JSONHandler struct {
	store database.JSONStore
}

func NewJSONHandler(store database.JSONStore) *JSONHandler {
	return &JSONHandler{store: store}
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
	if err := h.store.HealthCheck(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
	})
}

func getStorageMessage(isNew bool) string {
	if isNew {
		return "JSON document stored successfully"
	}
	return "JSON document already exists, returning existing ID"
}
