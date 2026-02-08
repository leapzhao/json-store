package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Environment string

const (
	EnvLocal   Environment = "local"
	EnvTest    Environment = "test"
	EnvProduct Environment = "product"
	EnvDefault Environment = "local"
)

type Config struct {
	Environment Environment `mapstructure:"environment"`

	Server struct {
		Port         string `mapstructure:"port"`
		Host         string `mapstructure:"host"`
		ReadTimeout  int    `mapstructure:"read_timeout"`
		WriteTimeout int    `mapstructure:"write_timeout"`
		IdleTimeout  int    `mapstructure:"idle_timeout"`
	} `mapstructure:"server"`

	Database struct {
		Type      string `mapstructure:"type"`
		Host      string `mapstructure:"host"`
		Port      int    `mapstructure:"port"`
		User      string `mapstructure:"user"`
		Password  string `mapstructure:"password"`
		Name      string `mapstructure:"name"`
		SSLMode   string `mapstructure:"ssl_mode"`
		MaxConns  int    `mapstructure:"max_conns"`
		IdleConns int    `mapstructure:"idle_conns"`
	} `mapstructure:"database"`

	Logging struct {
		Level      string `mapstructure:"level"`
		Format     string `mapstructure:"format"`
		OutputPath string `mapstructure:"output_path"`
	} `mapstructure:"logging"`

	Security struct {
		EnableHTTPS bool     `mapstructure:"enable_https"`
		CertFile    string   `mapstructure:"cert_file"`
		KeyFile     string   `mapstructure:"key_file"`
		CorsOrigins []string `mapstructure:"cors_origins"`
	} `mapstructure:"security"`
}

// LoadConfig 加载配置，支持多环境
func LoadConfig() (*Config, error) {
	// 确定环境
	env := GetEnvironment()

	// 设置配置文件路径
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "./config"
	}

	viper.SetConfigName(fmt.Sprintf("config.%s", env))
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configPath)
	viper.AddConfigPath(".")

	// 设置默认值
	setDefaults()

	// 读取环境变量（优先于配置文件）
	bindEnvVars()

	// 读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		// 如果配置文件不存在，仅使用环境变量和默认值
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
		fmt.Printf("Config file not found, using environment variables and defaults\n")
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 验证配置
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	fmt.Printf("Loaded configuration for environment: %s\n", env)

	return &config, nil
}

// GetEnvironment 获取当前环境
func GetEnvironment() Environment {
	env := os.Getenv("APP_ENV")
	if env == "" {
		env = string(EnvDefault)
	}

	switch strings.ToLower(env) {
	case "local", "dev", "development":
		return EnvLocal
	case "test", "staging":
		return EnvTest
	case "prod", "production":
		return EnvProduct
	default:
		fmt.Printf("Unknown environment: %s, using default: %s\n", env, EnvDefault)
		return EnvDefault
	}
}

// IsProduction 检查是否生产环境
func IsProduction() bool {
	return GetEnvironment() == EnvProduct
}

// IsTest 检查是否测试环境
func IsTest() bool {
	return GetEnvironment() == EnvTest
}

// IsLocal 检查是否本地环境
func IsLocal() bool {
	return GetEnvironment() == EnvLocal
}

func setDefaults() {
	viper.SetDefault("environment", EnvLocal)

	// 服务器配置默认值
	viper.SetDefault("server.port", "8080")
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.read_timeout", 10)
	viper.SetDefault("server.write_timeout", 10)
	viper.SetDefault("server.idle_timeout", 60)

	// 数据库默认值
	viper.SetDefault("database.type", "postgres")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.ssl_mode", "disable")
	viper.SetDefault("database.max_conns", 25)
	viper.SetDefault("database.idle_conns", 5)

	// 日志默认值
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "console")
	viper.SetDefault("logging.output_path", "stdout")

	// 安全默认值
	viper.SetDefault("security.enable_https", false)
	viper.SetDefault("security.cors_origins", []string{"*"})
}

func bindEnvVars() {
	// 绑定环境变量到配置项
	viper.BindEnv("environment", "APP_ENV")

	viper.BindEnv("server.port", "SERVER_PORT")
	viper.BindEnv("server.host", "SERVER_HOST")

	viper.BindEnv("database.type", "DB_TYPE")
	viper.BindEnv("database.host", "DB_HOST")
	viper.BindEnv("database.port", "DB_PORT")
	viper.BindEnv("database.user", "DB_USER")
	viper.BindEnv("database.password", "DB_PASSWORD")
	viper.BindEnv("database.name", "DB_NAME")
	viper.BindEnv("database.ssl_mode", "DB_SSL_MODE")

	viper.BindEnv("logging.level", "LOG_LEVEL")
	viper.BindEnv("logging.format", "LOG_FORMAT")
	viper.BindEnv("logging.output_path", "LOG_OUTPUT")

	viper.BindEnv("security.enable_https", "ENABLE_HTTPS")
	viper.BindEnv("security.cert_file", "CERT_FILE")
	viper.BindEnv("security.key_file", "KEY_FILE")
}

func validateConfig(cfg *Config) error {
	if cfg.Server.Port == "" {
		return fmt.Errorf("server port is required")
	}

	if cfg.Database.Host == "" || cfg.Database.Name == "" {
		return fmt.Errorf("database host and name are required")
	}

	return nil
}
