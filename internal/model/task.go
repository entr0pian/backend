package model

import "time"

type Task struct {
	ID          int    `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

type TaskEvent struct {
	ID           int       `json:"id,omitempty"`
	Action       string    `json:"action,omitempty"`
	Error        string    `json:"error,omitempty"`
	Reason       string    `json:"reason,omitempty"`
	SQSMessageID string    `json:"sqs_message_id,omitempty"`
	Timestamp    time.Time `json:"ts"`
	Level        string    `json:"level"`
}
