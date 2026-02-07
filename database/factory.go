package database

import (
	"fmt"
	"github.com/leapzhao/json-store/config"
)

// DatabaseType 数据库类型
type DatabaseType string

const (
	Postgres DatabaseType = "postgres"
	MySQL    DatabaseType = "mysql"
)

// CreateStore 工厂方法，根据配置创建对应的存储实例
func CreateStore(cfg config.Config) (JSONStore, error) {
	dbCfg := cfg.Database

	switch DatabaseType(dbCfg.Type) {
	case Postgres:
		return NewPostgresStore(
			dbCfg.Host,
			dbCfg.Port,
			dbCfg.User,
			dbCfg.Password,
			dbCfg.Name,
			dbCfg.SSLMode,
		)
	case MySQL:
		return NewMySQLStore(
			dbCfg.Host,
			dbCfg.Port,
			dbCfg.User,
			dbCfg.Password,
			dbCfg.Name,
		)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbCfg.Type)
	}
}
