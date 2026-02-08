package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/leapzhao/json-store/model"

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

func (s *PostgresStore) StoreJSONBatch(ctx context.Context, jsonDataList [][]byte) ([]*model.JSONDocument, error) {
	if len(jsonDataList) == 0 {
		return nil, fmt.Errorf("no JSON data provided")
	}

	if len(jsonDataList) > 100 {
		return nil, fmt.Errorf("batch size exceeds limit of 100")
	}

	// 开始事务
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	results := make([]*model.JSONDocument, 0, len(jsonDataList))

	// 批量插入
	for i, jsonData := range jsonDataList {
		// 验证JSON
		if !json.Valid(jsonData) {
			log.Warn().Int("index", i).Msg("Invalid JSON in batch, skipping")
			continue
		}

		hash := calculateHash(jsonData)
		size := int64(len(jsonData))
		id := uuid.New().String()

		// 检查是否已存在
		var existingID string
		err := tx.QueryRowContext(ctx,
			"SELECT id FROM json_documents WHERE content_hash = $1",
			hash,
		).Scan(&existingID)

		if err == nil {
			// 已存在，获取完整记录
			doc, err := s.GetJSONByID(ctx, existingID)
			if err == nil {
				results = append(results, doc)
				continue
			}
		}

		// 插入新记录
		query := `
			INSERT INTO json_documents (id, content_hash, json_data, size)
			VALUES ($1, $2, $3, $4)
			RETURNING id, content_hash, json_data, size, created_at, updated_at
		`

		var doc model.JSONDocument
		err = tx.QueryRowContext(ctx, query, id, hash, jsonData, size).Scan(
			&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
			&doc.CreatedAt, &doc.UpdatedAt,
		)

		if err != nil {
			log.Error().Err(err).Int("index", i).Msg("Failed to insert JSON in batch")
			// 继续处理其他记录
			continue
		}

		results = append(results, &doc)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info().Int("total", len(jsonDataList)).Int("success", len(results)).Msg("JSON batch stored")

	return results, nil
}

func (s *PostgresStore) GetJSONBatch(ctx context.Context, ids []string) ([]*model.JSONDocument, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("no IDs provided")
	}

	if len(ids) > 100 {
		return nil, fmt.Errorf("batch size exceeds limit of 100")
	}

	// 构建参数化查询
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, content_hash, json_data, size, created_at, updated_at, metadata
		FROM json_documents
		WHERE id IN (%s)
		ORDER BY created_at DESC
	`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query batch: %w", err)
	}
	defer rows.Close()

	documents := make([]*model.JSONDocument, 0, len(ids))
	for rows.Next() {
		var doc model.JSONDocument
		err := rows.Scan(
			&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
			&doc.CreatedAt, &doc.UpdatedAt, &doc.Metadata,
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to scan row in batch")
			continue
		}
		documents = append(documents, &doc)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return documents, nil
}

func (s *PostgresStore) GetStats(ctx context.Context) (*model.DatabaseStats, error) {
	stats := &model.DatabaseStats{}

	// 获取基础统计
	query := `
		SELECT 
			COUNT(*) as total_documents,
			COALESCE(SUM(size), 0) as total_size,
			COALESCE(AVG(size), 0) as avg_size,
			COALESCE(MAX(size), 0) as max_size,
			COALESCE(MIN(size), 0) as min_size,
			COUNT(DISTINCT content_hash) as unique_hashes,
			MAX(updated_at) as last_updated
		FROM json_documents
	`

	err := s.db.QueryRowContext(ctx, query).Scan(
		&stats.TotalDocuments, &stats.TotalSize, &stats.AverageSize,
		&stats.MaxSize, &stats.MinSize, &stats.UniqueHashes, &stats.LastUpdated,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	// 获取每日统计（最近7天）
	dailyQuery := `
		SELECT 
			DATE(created_at) as date,
			COUNT(*) as count,
			SUM(size) as size
		FROM json_documents
		WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
		GROUP BY DATE(created_at)
		ORDER BY date DESC
	`

	rows, err := s.db.QueryContext(ctx, dailyQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get daily stats")
	} else {
		defer rows.Close()

		dailyCounts := make([]model.DayCount, 0)
		for rows.Next() {
			var dc model.DayCount
			err := rows.Scan(&dc.Date, &dc.Count, &dc.Size)
			if err != nil {
				log.Error().Err(err).Msg("Failed to scan daily stats")
				continue
			}
			dailyCounts = append(dailyCounts, dc)
		}
		stats.DailyCounts = dailyCounts
	}

	return stats, nil
}

func (s *PostgresStore) GetMetrics(ctx context.Context) (*model.DatabaseMetrics, error) {
	metrics := &model.DatabaseMetrics{
		Timestamp: time.Now(),
	}

	// 获取数据库连接信息
	connQuery := `
		SELECT 
			numbackends as active_connections,
			setting::int as max_connections
		FROM pg_stat_database, pg_settings
		WHERE datname = current_database() 
			AND pg_settings.name = 'max_connections'
	`

	err := s.db.QueryRowContext(ctx, connQuery).Scan(
		&metrics.ActiveConnections, &metrics.MaxConnections,
	)

	if err != nil {
		log.Error().Err(err).Msg("Failed to get connection metrics")
	}

	// 获取缓存命中率
	cacheQuery := `
		SELECT 
			SUM(heap_blks_hit) / NULLIF(SUM(heap_blks_hit + heap_blks_read), 0) as hit_ratio
		FROM pg_statio_user_tables
	`

	var hitRatio sql.NullFloat64
	err = s.db.QueryRowContext(ctx, cacheQuery).Scan(&hitRatio)
	if err == nil && hitRatio.Valid {
		metrics.CacheHitRatio = hitRatio.Float64
	}

	// 获取表统计
	tableQuery := `
		SELECT 
			tablename as table_name,
			n_live_tup as rows,
			pg_table_size(schemaname || '.' || tablename) as table_size,
			pg_indexes_size(schemaname || '.' || tablename) as index_size,
			pg_total_relation_size(schemaname || '.' || tablename) as total_size
		FROM pg_stat_user_tables
		WHERE schemaname = 'public'
		ORDER BY tablename
	`

	rows, err := s.db.QueryContext(ctx, tableQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get table metrics")
	} else {
		defer rows.Close()

		tables := make([]model.TableStats, 0)
		for rows.Next() {
			var ts model.TableStats
			err := rows.Scan(&ts.Name, &ts.Rows, &ts.Size, &ts.IndexSize, &ts.TotalSize)
			if err != nil {
				log.Error().Err(err).Msg("Failed to scan table metrics")
				continue
			}
			tables = append(tables, ts)
		}
		metrics.Tables = tables
	}

	return metrics, nil
}
