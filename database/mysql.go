package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/leapzhao/json-store/model"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type MySQLStore struct {
	db *sql.DB
}

func NewMySQLStore(host string, port int, user, password, dbname string) (*MySQLStore, error) {
	connStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		user, password, host, port, dbname,
	)

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	store := &MySQLStore{db: db}

	// 执行迁移
	if err := store.Migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}

	log.Info().Msg("MySQL connection established")
	return store, nil
}

func (s *MySQLStore) Migrate() error {
	query := `
	CREATE TABLE IF NOT EXISTS json_documents (
		id VARCHAR(36) PRIMARY KEY,
		content_hash VARCHAR(64) UNIQUE NOT NULL,
		json_data JSON NOT NULL,
		size BIGINT NOT NULL DEFAULT 0,
		metadata JSON DEFAULT (JSON_OBJECT()),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_content_hash (content_hash),
		INDEX idx_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	
	-- MySQL 8.0+ 支持JSON索引
	ALTER TABLE json_documents
	ADD INDEX idx_json_data ((CAST(json_data AS CHAR(255))));
	`

	_, err := s.db.Exec(query)
	return err
}

func (s *MySQLStore) StoreJSON(ctx context.Context, jsonData []byte) (*model.JSONDocument, error) {
	// 验证JSON
	if !json.Valid(jsonData) {
		return nil, fmt.Errorf("invalid JSON data")
	}

	// 计算哈希值
	hash := calculateHash(jsonData)
	size := int64(len(jsonData))

	// 检查是否已存在
	if existing, err := s.GetJSONByHash(ctx, hash); err == nil {
		return existing, nil
	}

	// MySQL需要单独检查重复（使用ON DUPLICATE KEY UPDATE）
	id := uuid.New().String()
	query := `
		INSERT INTO json_documents (id, content_hash, json_data, size)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			updated_at = CURRENT_TIMESTAMP
	`

	result, err := s.db.ExecContext(ctx, query, id, hash, jsonData, size)
	if err != nil {
		return nil, fmt.Errorf("failed to store JSON: %w", err)
	}

	// 如果是更新，获取已有ID
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// 重复插入，获取已有记录
		return s.GetJSONByHash(ctx, hash)
	}

	// 获取新插入的记录
	doc, err := s.GetJSONByID(ctx, id)
	if err != nil {
		return nil, err
	}

	log.Info().
		Str("id", doc.ID).
		Str("hash", hash).
		Int64("size", size).
		Msg("JSON stored in MySQL")

	return doc, nil
}

func (s *MySQLStore) GetJSONByID(ctx context.Context, id string) (*model.JSONDocument, error) {
	query := `
		SELECT id, content_hash, json_data, size, created_at, updated_at, metadata
		FROM json_documents
		WHERE id = ?
	`

	var doc model.JSONDocument
	var metadataStr sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
		&doc.CreatedAt, &doc.UpdatedAt, &metadataStr,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document not found with id: %s", id)
		}
		return nil, fmt.Errorf("failed to get JSON: %w", err)
	}

	// 解析metadata
	if metadataStr.Valid && metadataStr.String != "" {
		if err := json.Unmarshal([]byte(metadataStr.String), &doc.Metadata); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal metadata")
		}
	}

	return &doc, nil
}

func (s *MySQLStore) GetJSONByHash(ctx context.Context, hash string) (*model.JSONDocument, error) {
	query := `
		SELECT id, content_hash, json_data, size, created_at, updated_at, metadata
		FROM json_documents
		WHERE content_hash = ?
		LIMIT 1
	`

	var doc model.JSONDocument
	var metadataStr sql.NullString

	err := s.db.QueryRowContext(ctx, query, hash).Scan(
		&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
		&doc.CreatedAt, &doc.UpdatedAt, &metadataStr,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document not found with hash: %s", hash)
		}
		return nil, fmt.Errorf("failed to get JSON by hash: %w", err)
	}

	// 解析metadata
	if metadataStr.Valid && metadataStr.String != "" {
		if err := json.Unmarshal([]byte(metadataStr.String), &doc.Metadata); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal metadata")
		}
	}

	return &doc, nil
}

func (s *MySQLStore) HealthCheck(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *MySQLStore) Close() error {
	return s.db.Close()
}
