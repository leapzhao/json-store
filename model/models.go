package model

import (
	"time"
)

type JSONDocument struct {
	ID          string         `json:"id"`
	ContentHash string         `json:"content_hash"`
	JSONData    []byte         `json:"json_data"`
	Size        int64          `json:"size"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type StoreRequest struct {
	JSONData []byte         `json:"json_data" validate:"required"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type StoreBatchRequest struct {
	Documents []StoreRequest `json:"documents" validate:"required,min=1,max=100"`
}

type StoreResponse struct {
	ID        string    `json:"id"`
	IsNew     bool      `json:"is_new"`
	CreatedAt time.Time `json:"created_at"`
	Message   string    `json:"message,omitempty"`
}

type StoreBatchResponse struct {
	SuccessCount int             `json:"success_count"`
	FailureCount int             `json:"failure_count"`
	TotalCount   int             `json:"total_count"`
	Results      []StoreResponse `json:"results"`
	Failures     []BatchFailure  `json:"failures,omitempty"`
	Duration     time.Duration   `json:"duration_ms"`
}

type BatchFailure struct {
	Index   int    `json:"index"`
	Error   string `json:"error"`
	Message string `json:"message"`
}

type GetBatchRequest struct {
	IDs []string `json:"ids" validate:"required,min=1,max=100"`
}

type GetBatchResponse struct {
	SuccessCount int            `json:"success_count"`
	FailureCount int            `json:"failure_count"`
	Documents    []JSONDocument `json:"documents"`
	Failures     []BatchFailure `json:"failures,omitempty"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type DatabaseStats struct {
	TotalDocuments int64      `json:"total_documents"`
	TotalSize      int64      `json:"total_size_bytes"`
	AverageSize    float64    `json:"average_size_bytes"`
	MaxSize        int64      `json:"max_size_bytes"`
	MinSize        int64      `json:"min_size_bytes"`
	DailyCounts    []DayCount `json:"daily_counts,omitempty"`
	UniqueHashes   int64      `json:"unique_hashes"`
	LastUpdated    time.Time  `json:"last_updated"`
}

type DayCount struct {
	Date  string `json:"date"`
	Count int64  `json:"count"`
	Size  int64  `json:"size_bytes"`
}

type DatabaseMetrics struct {
	Uptime            time.Duration `json:"uptime_seconds"`
	ActiveConnections int           `json:"active_connections"`
	MaxConnections    int           `json:"max_connections"`
	CacheHitRatio     float64       `json:"cache_hit_ratio,omitempty"`
	QueryPerSecond    float64       `json:"queries_per_second"`
	SlowQueries       int64         `json:"slow_queries"`
	Tables            []TableStats  `json:"tables,omitempty"`
	Timestamp         time.Time     `json:"timestamp"`
}

type TableStats struct {
	Name      string `json:"name"`
	Rows      int64  `json:"rows"`
	Size      int64  `json:"size_bytes"`
	IndexSize int64  `json:"index_size_bytes"`
	TotalSize int64  `json:"total_size_bytes"`
}

type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Database  bool      `json:"database"`
	Version   string    `json:"version,omitempty"`
}

type ReadyResponse struct {
	Ready     bool          `json:"ready"`
	Timestamp time.Time     `json:"timestamp"`
	Checks    []HealthCheck `json:"checks,omitempty"`
}

type HealthCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type VersionResponse struct {
	Version     string `json:"version"`
	BuildTime   string `json:"build_time,omitempty"`
	GitCommit   string `json:"git_commit,omitempty"`
	Environment string `json:"environment"`
	GoVersion   string `json:"go_version"`
}
