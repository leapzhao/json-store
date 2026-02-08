package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// NormalizeJSON 规范化JSON（排序键名、去除空格）
func NormalizeJSON(data []byte) ([]byte, error) {
	// 尝试解码为通用类型
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}

	// 重新编码，确保键名排序一致
	return json.Marshal(obj)
}

// CalculateHash 计算JSON哈希值
func CalculateHash(data []byte) (string, error) {
	normalized, err := NormalizeJSON(data)
	if err != nil {
		// 如果无法规范化，使用原始数据
		normalized = data
	}

	hash := sha256.Sum256(normalized)
	return hex.EncodeToString(hash[:]), nil
}

// ValidateJSON 验证JSON格式
func ValidateJSON(data []byte) bool {
	return json.Valid(data)
}

// FormatBytes 格式化字节大小为易读格式
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// StringInSlice 检查字符串是否在切片中
func StringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

// ParseCommaSeparatedIDs 解析逗号分隔的ID字符串
func ParseCommaSeparatedIDs(idsStr string) []string {
	if idsStr == "" {
		return []string{}
	}

	ids := strings.Split(idsStr, ",")
	result := make([]string, 0, len(ids))

	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
