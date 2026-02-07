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

type StoreResponse struct {
	ID        string    `json:"id"`
	IsNew     bool      `json:"is_new"`
	CreatedAt time.Time `json:"created_at"`
	Message   string    `json:"message,omitempty"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}
