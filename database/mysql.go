package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/leapzhao/json-store/model"
	"strings"
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

func (s *MySQLStore) StoreJSONBatch(ctx context.Context, jsonDataList [][]byte) ([]*model.JSONDocument, error) {
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

		// 检查是否已存在
		var existingID string
		err := tx.QueryRowContext(ctx,
			"SELECT id FROM json_documents WHERE content_hash = ?",
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
		id := uuid.New().String()
		query := `
			INSERT INTO json_documents (id, content_hash, json_data, size)
			VALUES (?, ?, ?, ?)
		`

		_, err = tx.ExecContext(ctx, query, id, hash, jsonData, size)
		if err != nil {
			log.Error().Err(err).Int("index", i).Msg("Failed to insert JSON in batch")
			continue
		}

		// 获取插入的记录
		doc, err := s.GetJSONByID(ctx, id)
		if err != nil {
			log.Error().Err(err).Str("id", id).Msg("Failed to get inserted document")
			continue
		}

		results = append(results, doc)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info().Int("total", len(jsonDataList)).Int("success", len(results)).Msg("JSON batch stored")

	return results, nil
}

func (s *MySQLStore) GetJSONBatch(ctx context.Context, ids []string) ([]*model.JSONDocument, error) {
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
		placeholders[i] = "?"
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
		var metadataStr sql.NullString

		err := rows.Scan(
			&doc.ID, &doc.ContentHash, &doc.JSONData, &doc.Size,
			&doc.CreatedAt, &doc.UpdatedAt, &metadataStr,
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to scan row in batch")
			continue
		}

		// 解析metadata
		if metadataStr.Valid && metadataStr.String != "" {
			if err := json.Unmarshal([]byte(metadataStr.String), &doc.Metadata); err != nil {
				log.Error().Err(err).Msg("Failed to unmarshal metadata")
			}
		}

		documents = append(documents, &doc)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return documents, nil
}

func (s *MySQLStore) GetStats(ctx context.Context) (*model.DatabaseStats, error) {
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
		WHERE created_at >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
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

func (s *MySQLStore) GetMetrics(ctx context.Context) (*model.DatabaseMetrics, error) {
	metrics := &model.DatabaseMetrics{
		Timestamp: time.Now(),
	}

	// 获取连接信息
	connQuery := `
		SHOW STATUS LIKE 'Threads_connected'
	`

	var varName string
	var value string
	err := s.db.QueryRowContext(ctx, connQuery).Scan(&varName, &value)
	if err == nil {
		var threads int
		fmt.Sscanf(value, "%d", &threads)
		metrics.ActiveConnections = threads
	}

	// 获取最大连接数
	maxConnQuery := `
		SHOW VARIABLES LIKE 'max_connections'
	`

	err = s.db.QueryRowContext(ctx, maxConnQuery).Scan(&varName, &value)
	if err == nil {
		var maxConn int
		fmt.Sscanf(value, "%d", &maxConn)
		metrics.MaxConnections = maxConn
	}

	// 获取慢查询数量
	slowQuery := `
		SHOW GLOBAL STATUS LIKE 'Slow_queries'
	`

	err = s.db.QueryRowContext(ctx, slowQuery).Scan(&varName, &value)
	if err == nil {
		var slowQueries int64
		fmt.Sscanf(value, "%d", &slowQueries)
		metrics.SlowQueries = slowQueries
	}

	// 获取表统计
	tableQuery := `
		SELECT 
			TABLE_NAME as table_name,
			TABLE_ROWS as rows,
			DATA_LENGTH as data_size,
			INDEX_LENGTH as index_size,
			DATA_LENGTH + INDEX_LENGTH as total_size
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME
	`

	rows, err := s.db.QueryContext(ctx, tableQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get table metrics")
	} else {
		defer rows.Close()

		tables := make([]model.TableStats, 0)
		for rows.Next() {
			var ts model.TableStats
			var rowsStr sql.NullString
			var dataSize, indexSize, totalSize sql.NullString

			err := rows.Scan(&ts.Name, &rowsStr, &dataSize, &indexSize, &totalSize)
			if err != nil {
				log.Error().Err(err).Msg("Failed to scan table metrics")
				continue
			}

			// 转换数据
			if rowsStr.Valid {
				fmt.Sscanf(rowsStr.String, "%d", &ts.Rows)
			}
			if dataSize.Valid {
				fmt.Sscanf(dataSize.String, "%d", &ts.Size)
			}
			if indexSize.Valid {
				fmt.Sscanf(indexSize.String, "%d", &ts.IndexSize)
			}
			if totalSize.Valid {
				fmt.Sscanf(totalSize.String, "%d", &ts.TotalSize)
			}

			tables = append(tables, ts)
		}
		metrics.Tables = tables
	}

	return metrics, nil
}
