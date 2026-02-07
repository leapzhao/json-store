package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// 规范化JSON（排序键名、去除空格）
func NormalizeJSON(data []byte) ([]byte, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}

	// 重新编码，确保键名排序一致
	return json.Marshal(obj)
}

// 计算JSON哈希值
func CalculateHash(data []byte) (string, error) {
	normalized, err := NormalizeJSON(data)
	if err != nil {
		// 如果无法规范化，使用原始数据
		normalized = data
	}

	hash := sha256.Sum256(normalized)
	return hex.EncodeToString(hash[:]), nil
}
