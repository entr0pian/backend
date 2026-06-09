package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"taskapp/backend/internal/model"
)

type TaskHandler struct {
	db        *sql.DB
	taskQueue chan model.Task
	logQueue  chan model.TaskEvent
}

func NewTaskHandler(db *sql.DB, taskQueue chan model.Task, logQueue chan model.TaskEvent) *TaskHandler {
	return &TaskHandler{db: db, taskQueue: taskQueue, logQueue: logQueue}
}

func (h *TaskHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.get(w, r)
	case http.MethodPost:
		h.create(w, r)
	case http.MethodPut:
		h.update(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *TaskHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Ping(); err != nil {
		http.Error(w, "db not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

func (h *TaskHandler) get(w http.ResponseWriter, _ *http.Request) {
	rows, err := h.db.Query("SELECT id, title, description, status FROM tasks")
	if err != nil {
		http.Error(w, "failed to query tasks", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tasks []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.ID, &t.Title, &t.Description, &t.Status); err != nil {
			http.Error(w, "failed to scan task", http.StatusInternalServerError)
			return
		}
		tasks = append(tasks, t)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (h *TaskHandler) create(w http.ResponseWriter, r *http.Request) {
	var t model.Task
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if t.Title == "" {
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}
	if t.Status == "" {
		t.Status = "todo"
	}

	err := h.db.QueryRow(`
		INSERT INTO tasks (title, description, status)
		VALUES ($1, $2, $3)
		RETURNING id
	`, t.Title, t.Description, t.Status).Scan(&t.ID)
	if err != nil {
		http.Error(w, "failed to create task", http.StatusInternalServerError)
		return
	}

	h.taskQueue <- t
	h.logQueue <- model.TaskEvent{ID: t.ID, Action: "created", Level: "info", Timestamp: time.Now()}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(t)
}

func (h *TaskHandler) update(w http.ResponseWriter, r *http.Request) {
	var t model.Task
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if t.ID == 0 {
		h.logQueue <- model.TaskEvent{Action: "update_failed_no_id", Level: "error", Timestamp: time.Now()}
		http.Error(w, "task id is required", http.StatusBadRequest)
		return
	}

	result, err := h.db.Exec(`
		UPDATE tasks SET title = $1, description = $2, status = $3 WHERE id = $4
	`, t.Title, t.Description, t.Status, t.ID)
	if err != nil {
		http.Error(w, "failed to update task", http.StatusInternalServerError)
		return
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	h.logQueue <- model.TaskEvent{ID: t.ID, Action: "updated", Level: "info", Timestamp: time.Now()}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("task updated"))
}
