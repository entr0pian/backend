package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"taskapp/backend/internal/model"
)

// fakeSQSClient lets tests script ReceiveMessage/DeleteMessage responses
// without talking to real AWS.
type fakeSQSClient struct {
	receiveOutputs []*sqs.ReceiveMessageOutput
	receiveErrs    []error
	receiveCalls   int

	deleteCalls int
}

func (f *fakeSQSClient) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	i := f.receiveCalls
	f.receiveCalls++

	var out *sqs.ReceiveMessageOutput
	if i < len(f.receiveOutputs) {
		out = f.receiveOutputs[i]
	} else {
		out = &sqs.ReceiveMessageOutput{}
	}

	var err error
	if i < len(f.receiveErrs) {
		err = f.receiveErrs[i]
	}
	return out, err
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.deleteCalls++
	return &sqs.DeleteMessageOutput{}, nil
}

// drainOne reads a single event off logQueue, failing the test if none
// arrives within the timeout.
func drainOne(t *testing.T, logQueue chan model.TaskEvent) model.TaskEvent {
	t.Helper()
	select {
	case ev := <-logQueue:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log event")
		return model.TaskEvent{}
	}
}

func TestSQSWorker_ReceiveError(t *testing.T) {
	client := &fakeSQSClient{
		receiveErrs: []error{errors.New("AccessDenied: not authorized to perform sqs:ReceiveMessage")},
	}
	logQueue := make(chan model.TaskEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go SQSWorker(ctx, client, "https://sqs.example/queue", nil, logQueue)

	drainOne(t, logQueue) // sqs_worker_started

	ev := drainOne(t, logQueue)
	if ev.Action != "sqs_receive_error" {
		t.Fatalf("expected action sqs_receive_error, got %q", ev.Action)
	}
	if ev.Error == "" {
		t.Fatal("expected Error to contain the underlying AWS SDK error, got empty string")
	}
}

func TestSQSWorker_InvalidMessage_JSONParseError(t *testing.T) {
	client := &fakeSQSClient{
		receiveOutputs: []*sqs.ReceiveMessageOutput{
			{
				Messages: []types.Message{
					{MessageId: aws.String("msg-1"), Body: aws.String("not-json")},
				},
			},
		},
	}
	logQueue := make(chan model.TaskEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go SQSWorker(ctx, client, "https://sqs.example/queue", nil, logQueue)

	drainOne(t, logQueue) // sqs_worker_started

	ev := drainOne(t, logQueue)
	if ev.Action != "sqs_invalid_message" {
		t.Fatalf("expected action sqs_invalid_message, got %q", ev.Action)
	}
	if ev.Reason != "json_parse_error" {
		t.Fatalf("expected reason json_parse_error, got %q", ev.Reason)
	}
	if ev.Error == "" {
		t.Fatal("expected Error to contain the JSON unmarshal error, got empty string")
	}
	if ev.SQSMessageID != "msg-1" {
		t.Fatalf("expected sqs_message_id msg-1, got %q", ev.SQSMessageID)
	}
}

func TestSQSWorker_InvalidMessage_MissingTitle(t *testing.T) {
	client := &fakeSQSClient{
		receiveOutputs: []*sqs.ReceiveMessageOutput{
			{
				Messages: []types.Message{
					{MessageId: aws.String("msg-2"), Body: aws.String(`{"description":"no title here"}`)},
				},
			},
		},
	}
	logQueue := make(chan model.TaskEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go SQSWorker(ctx, client, "https://sqs.example/queue", nil, logQueue)

	drainOne(t, logQueue) // sqs_worker_started

	ev := drainOne(t, logQueue)
	if ev.Action != "sqs_invalid_message" {
		t.Fatalf("expected action sqs_invalid_message, got %q", ev.Action)
	}
	if ev.Reason != "missing_title" {
		t.Fatalf("expected reason missing_title, got %q", ev.Reason)
	}
	if ev.Error != "" {
		t.Fatalf("expected no Error for missing_title (nothing failed to parse), got %q", ev.Error)
	}
	if ev.SQSMessageID != "msg-2" {
		t.Fatalf("expected sqs_message_id msg-2, got %q", ev.SQSMessageID)
	}
}

func TestSQSWorker_DBInsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`INSERT INTO tasks`).
		WithArgs("a title", "").
		WillReturnError(errors.New("pq: relation \"tasks\" does not exist"))

	client := &fakeSQSClient{
		receiveOutputs: []*sqs.ReceiveMessageOutput{
			{
				Messages: []types.Message{
					{MessageId: aws.String("msg-3"), Body: aws.String(`{"title":"a title"}`)},
				},
			},
		},
	}
	logQueue := make(chan model.TaskEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go SQSWorker(ctx, client, "https://sqs.example/queue", db, logQueue)

	drainOne(t, logQueue) // sqs_worker_started

	ev := drainOne(t, logQueue)
	if ev.Action != "sqs_db_insert_error" {
		t.Fatalf("expected action sqs_db_insert_error, got %q", ev.Action)
	}
	if ev.Error == "" {
		t.Fatal("expected Error to contain the DB error message, got empty string")
	}
	if ev.SQSMessageID != "msg-3" {
		t.Fatalf("expected sqs_message_id msg-3, got %q", ev.SQSMessageID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}
