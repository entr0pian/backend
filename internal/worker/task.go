package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"taskapp/backend/internal/model"
)

func envInt32(key string, fallback int32) int32 {
	if v, err := strconv.ParseInt(os.Getenv(key), 10, 32); err == nil {
		return int32(v)
	}
	return fallback
}

// TaskWorker drains the internal task channel produced by the HTTP handler.
func TaskWorker(taskQueue chan model.Task, logQueue chan model.TaskEvent) {
	for task := range taskQueue {
		logQueue <- model.TaskEvent{ID: task.ID, Action: "task_received", Level: "info", Timestamp: time.Now()}
	}
}

// SQSWorker polls the given queue and inserts each message as a TODO task.
// It runs until ctx is cancelled.
func SQSWorker(ctx context.Context, client *sqs.Client, queueURL string, database *sql.DB, logQueue chan model.TaskEvent) {
	maxMessages := envInt32("SQS_MAX_MESSAGES", 1)
	workSleep := time.Duration(envInt32("SQS_WORK_SLEEP_SECONDS", 1)) * time.Second

	logQueue <- model.TaskEvent{Action: "sqs_worker_started", Level: "info", Timestamp: time.Now()}

	for {
		if ctx.Err() != nil {
			return
		}

		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: maxMessages,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logQueue <- model.TaskEvent{Action: "sqs_receive_error", Level: "error", Timestamp: time.Now()}
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range out.Messages {
			var t model.Task
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &t); err != nil || t.Title == "" {
				logQueue <- model.TaskEvent{Action: "sqs_invalid_message", Level: "error", Timestamp: time.Now()}
				continue
			}

			if err := database.QueryRowContext(ctx,
				`INSERT INTO tasks (title, description, status) VALUES ($1, $2, 'todo') RETURNING id`,
				t.Title, t.Description,
			).Scan(&t.ID); err != nil {
				logQueue <- model.TaskEvent{Action: "sqs_db_insert_error", Level: "error", Timestamp: time.Now()}
				continue
			}

			client.DeleteMessage(ctx, &sqs.DeleteMessageInput{ //nolint:errcheck
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})

			logQueue <- model.TaskEvent{ID: t.ID, Action: "sqs_task_created", Level: "info", Timestamp: time.Now()}

			time.Sleep(workSleep)
		}
	}
}
