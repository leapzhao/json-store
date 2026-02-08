package database

import (
	"context"

	"github.com/leapzhao/json-store/model"
)

// JSONStore 存储接口定义
type JSONStore interface {
	// StoreJSON 存储JSON，如果已存在则返回已有ID
	StoreJSON(ctx context.Context, jsonData []byte) (*model.JSONDocument, error)

	// StoreJSONBatch 批量存储JSON
	StoreJSONBatch(ctx context.Context, jsonDataList [][]byte) ([]*model.JSONDocument, error)

	// GetJSONByID 根据ID获取JSON
	GetJSONByID(ctx context.Context, id string) (*model.JSONDocument, error)

	// GetJSONBatch 批量获取JSON
	GetJSONBatch(ctx context.Context, ids []string) ([]*model.JSONDocument, error)

	// GetJSONByHash 根据哈希值获取JSON
	GetJSONByHash(ctx context.Context, hash string) (*model.JSONDocument, error)

	// GetStats 获取统计信息
	GetStats(ctx context.Context) (*model.DatabaseStats, error)

	// GetMetrics 获取性能指标
	GetMetrics(ctx context.Context) (*model.DatabaseMetrics, error)

	// Close 关闭数据库连接
	Close() error

	// HealthCheck 健康检查
	HealthCheck(ctx context.Context) error

	// Migrate 数据库迁移
	Migrate() error
}
