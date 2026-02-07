package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/leapzhao/json-store/model"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(host string, port int, user, password, dbname, sslmode string) (*PostgresStore, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	store := &PostgresStore{db: db}

	// 执行迁移
	if err := store.Migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate: %w", err)
	}

	log.Info().Msg("PostgreSQL connection established")
	return store, nil
}

func (s *PostgresStore) Migrate() error {
	query := `
	CREATE TABLE IF NOT EXISTS json_documents (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		content_hash VARCHAR(64) UNIQUE NOT NULL,
		json_data JSONB NOT NULL,
		size BIGINT NOT NULL DEFAULT 0,
		metadata JSONB DEFAULT '{}',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_content_hash ON json_documents(content_hash);
	CREATE INDEX IF NOT EXISTS idx_json_data_gin ON json_documents USING GIN(json_data);
	CREATE INDEX IF NOT EXISTS idx_created_at ON json_documents(created_at);
	
	CREATE OR REPLACE FUNCTION update_updated_at_column()
	RETURNS TRIGGER AS $$
	BEGIN
		NEW.updated_at = CURRENT_TIMESTAMP;
		RETURN NEW;
	END;
	$$ language 'plpgsql';
	
	DROP TRIGGER IF EXISTS update_json_documents_updated_at ON json_documents;
	CREATE TRIGGER update_json_documents_updated_at
		BEFORE UPDATE ON json_documents
		FOR EACH ROW
		EXECUTE FUNCTION update_updated_at_column();
	`

	_, err := s.db.Exec(query)
	return err
}

func (s *PostgresStore) StoreJSON(ctx context.Context, jsonData []byte) (*model.JSONDocument, error) {
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

	// 插入新记录
	id := uuid.New().String()
	query := `
		INSERT INTO json_documents (id, content_hash, json_data, size)
		VALUES ($1, $2, $3, $4)
		RETURNING id, content_hash, json_data, size, created_at, updated_at
	`

	var doc model.JSONDocument
	err := s.db.QueryRowContext(ctx, query, id, hash, jsonData, size).Scan(
		&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size, &doc.CreatedAt, &doc.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to store JSON: %w", err)
	}

	log.Info().
		Str("id", doc.ID).
		Str("hash", hash).
		Int64("size", size).
		Msg("JSON stored in PostgreSQL")

	return &doc, nil
}

func (s *PostgresStore) GetJSONByID(ctx context.Context, id string) (*model.JSONDocument, error) {
	query := `
		SELECT id, content_hash, json_data, size, created_at, updated_at, metadata
		FROM json_documents
		WHERE id = $1
	`

	var doc model.JSONDocument
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
		&doc.CreatedAt, &doc.UpdatedAt, &doc.Metadata,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document not found with id: %s", id)
		}
		return nil, fmt.Errorf("failed to get JSON: %w", err)
	}

	return &doc, nil
}

func (s *PostgresStore) GetJSONByHash(ctx context.Context, hash string) (*model.JSONDocument, error) {
	query := `
		SELECT id, content_hash, json_data, size, created_at, updated_at, metadata
		FROM json_documents
		WHERE content_hash = $1
		LIMIT 1
	`

	var doc model.JSONDocument
	err := s.db.QueryRowContext(ctx, query, hash).Scan(
		&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
		&doc.CreatedAt, &doc.UpdatedAt, &doc.Metadata,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("document not found with hash: %s", hash)
		}
		return nil, fmt.Errorf("failed to get JSON by hash: %w", err)
	}

	return &doc, nil
}

func (s *PostgresStore) HealthCheck(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
