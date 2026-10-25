package core

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// HeartbeatConfig holds runtime heartbeat settings for a single project.
type HeartbeatConfig struct {
	Enabled      bool
	IntervalMins int
	OnlyWhenIdle bool
	SessionKey   string
	Prompt       string // explicit prompt; empty = read HEARTBEAT.md
	Silent       bool   // suppress "💓" notification
	TimeoutMins  int
}

// HeartbeatStatus is returned by the /heartbeat command.
type HeartbeatStatus struct {
	Enabled      bool
	Paused       bool
	IntervalMins int
	OnlyWhenIdle bool
	SessionKey   string
	Silent       bool
	RunCount     int
	ErrorCount   int
	SkippedBusy  int
	LastRun      time.Time
	LastError    string
}

// heartbeatPersisted is the JSON-serialisable per-project state.
type heartbeatPersisted struct {
	Paused       bool `json:"paused"`
	IntervalMins int  `json:"interval_mins,omitempty"`
}

// HeartbeatScheduler manages periodic heartbeat execution across projects.
type HeartbeatScheduler struct {
	mu        sync.Mutex
	entries   map[string]*heartbeatEntry // project name → entry
	stopCh    chan struct{}
	stopped   bool
	stateFile string // path to heartbeat_state.json; empty = no persistence
	inflight  sync.WaitGroup
}

type heartbeatEntry struct {
	project string
	config  HeartbeatConfig
	engine  *Engine
	workDir string
	ticker  *time.Ticker
	stopCh  chan struct{}
	paused  bool

	origIntervalMins int // interval from config, for detecting overrides

	// Runtime stats
	runCount    int
	errorCount  int
	skippedBusy int
	lastRun     time.Time
	lastError   string
}

type heartbeatExecution struct {
	entry   *heartbeatEntry
	project string
	config  HeartbeatConfig
	engine  *Engine
	workDir string
}

func NewHeartbeatScheduler(dataDir string) *HeartbeatScheduler {
	hs := &HeartbeatScheduler{
		entries: make(map[string]*heartbeatEntry),
		stopCh:  make(chan struct{}),
	}
	if dataDir != "" {
		hs.stateFile = filepath.Join(dataDir, "heartbeat_state.json")
	}
	return hs
}

// Register adds a heartbeat entry for a project. Call before Start().
func (hs *HeartbeatScheduler) Register(project string, cfg HeartbeatConfig, engine *Engine, workDir string) {
	if !cfg.Enabled || cfg.SessionKey == "" {
		return
	}
	if cfg.IntervalMins <= 0 {
		cfg.IntervalMins = 30
	}
	if cfg.TimeoutMins <= 0 {
		cfg.TimeoutMins = 30
	}
	hs.mu.Lock()
	defer hs.mu.Unlock()

	entry := &heartbeatEntry{
		project:          project,
		config:           cfg,
		engine:           engine,
		workDir:          workDir,
		stopCh:           make(chan struct{}),
		origIntervalMins: cfg.IntervalMins,
	}

	// Restore persisted overrides
	if saved := hs.loadProjectState(project); saved != nil {
		entry.paused = saved.Paused
		if saved.IntervalMins > 0 {
			entry.config.IntervalMins = saved.IntervalMins
		}
	}

	hs.entries[project] = entry
}

// Start begins all registered heartbeat tickers.
func (hs *HeartbeatScheduler) Start() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.stopped {
		return
	}
	for _, entry := range hs.entries {
		hs.startEntry(entry)
	}
	if len(hs.entries) > 0 {
		slog.Info("heartbeat: scheduler started", "entries", len(hs.entries))
	}
}

func (hs *HeartbeatScheduler) startEntry(entry *heartbeatEntry) {
	interval := time.Duration(entry.config.IntervalMins) * time.Minute
	entry.ticker = time.NewTicker(interval)
	go hs.run(entry)
	state := "running"
	if entry.paused {
		state = "paused"
	}
	slog.Info("heartbeat: started",
		"project", entry.project,
		"interval", interval,
		"state", state,
		"session_key", entry.config.SessionKey,
		"only_when_idle", entry.config.OnlyWhenIdle,
	)
}

// CloseAdmission halts heartbeat tickers and rejects new manual triggers without
// waiting for already accepted runs. Call Wait after stopping engines so producer
// work blocked inside Engine can observe cancellation and drain.
func (hs *HeartbeatScheduler) CloseAdmission() {
	hs.mu.Lock()
	if hs.stopped {
		hs.mu.Unlock()
		return
	}
	hs.stopped = true
	close(hs.stopCh)
	for _, entry := range hs.entries {
		if entry.ticker != nil {
			entry.ticker.Stop()
		}
		select {
		case <-entry.stopCh:
		default:
			close(entry.stopCh)
		}
	}
	hs.mu.Unlock()
}

func (hs *HeartbeatScheduler) Wait() {
	hs.inflight.Wait()
}

// Stop halts all heartbeat tickers and waits for already accepted runs to finish.
func (hs *HeartbeatScheduler) Stop() {
	hs.CloseAdmission()
	hs.Wait()
}

// Status returns the heartbeat status for a project.
func (hs *HeartbeatScheduler) Status(project string) *HeartbeatStatus {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	entry, ok := hs.entries[project]
	if !ok {
		return nil
	}
	return &HeartbeatStatus{
		Enabled:      entry.config.Enabled,
		Paused:       entry.paused,
		IntervalMins: entry.config.IntervalMins,
		OnlyWhenIdle: entry.config.OnlyWhenIdle,
		SessionKey:   entry.config.SessionKey,
		Silent:       entry.config.Silent,
		RunCount:     entry.runCount,
		ErrorCount:   entry.errorCount,
		SkippedBusy:  entry.skippedBusy,
		LastRun:      entry.lastRun,
		LastError:    entry.lastError,
	}
}

// Pause temporarily stops heartbeat for a project without removing it.
func (hs *HeartbeatScheduler) Pause(project string) bool {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	entry, ok := hs.entries[project]
	if !ok {
		return false
	}
	if entry.paused {
		return true
	}
	entry.paused = true
	if entry.ticker != nil {
		entry.ticker.Stop()
	}
	slog.Info("heartbeat: paused", "project", project)
	hs.persistLocked()
	return true
}

// Resume resumes a paused heartbeat.
func (hs *HeartbeatScheduler) Resume(project string) bool {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.stopped {
		return false
	}
	entry, ok := hs.entries[project]
	if !ok {
		return false
	}
	if !entry.paused {
		return true
	}
	entry.paused = false
	interval := time.Duration(entry.config.IntervalMins) * time.Minute
	if entry.ticker != nil {
		entry.ticker.Reset(interval)
	}
	slog.Info("heartbeat: resumed", "project", project, "interval", interval)
	hs.persistLocked()
	return true
}

// SetInterval changes the heartbeat interval for a project.
func (hs *HeartbeatScheduler) SetInterval(project string, mins int) bool {
	if mins <= 0 {
		return false
	}
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.stopped {
		return false
	}
	entry, ok := hs.entries[project]
	if !ok {
		return false
	}
	entry.config.IntervalMins = mins
	interval := time.Duration(mins) * time.Minute
	if entry.ticker != nil && !entry.paused {
		entry.ticker.Reset(interval)
	}
	slog.Info("heartbeat: interval changed", "project", project, "interval", interval)
	hs.persistLocked()
	return true
}

// TriggerNow executes a heartbeat immediately (async).
func (hs *HeartbeatScheduler) TriggerNow(project string) bool {
	hs.mu.Lock()
	entry, ok := hs.entries[project]
	if !ok || hs.stopped {
		hs.mu.Unlock()
		return false
	}
	exec := heartbeatExecution{
		entry:   entry,
		project: entry.project,
		config:  entry.config,
		engine:  entry.engine,
		workDir: entry.workDir,
	}
	hs.inflight.Add(1)
	hs.mu.Unlock()
	go func() {
		defer hs.inflight.Done()
		hs.execute(exec)
	}()
	return true
}

// ── persistence ──────────────────────────────────────────────

// loadProjectState reads persisted state for a single project.
// Must NOT hold hs.mu when reading the file (called during Register which already holds it,
// but file I/O here is acceptable because Register is called sequentially at startup).
func (hs *HeartbeatScheduler) loadProjectState(project string) *heartbeatPersisted {
	if hs.stateFile == "" {
		return nil
	}
	data, err := os.ReadFile(hs.stateFile)
	if err != nil {
		return nil
	}
	var states map[string]*heartbeatPersisted
	if err := json.Unmarshal(data, &states); err != nil {
		return nil
	}
	return states[project]
}

// persistLocked saves all project overrides to disk. Caller must hold hs.mu.
func (hs *HeartbeatScheduler) persistLocked() {
	if hs.stateFile == "" {
		return
	}
	states := make(map[string]*heartbeatPersisted, len(hs.entries))
	needSave := false
	for name, entry := range hs.entries {
		p := &heartbeatPersisted{Paused: entry.paused}
		if entry.config.IntervalMins != entry.origIntervalMins {
			p.IntervalMins = entry.config.IntervalMins
		}
		if p.Paused || p.IntervalMins > 0 {
			states[name] = p
			needSave = true
		}
	}
	if !needSave {
		os.Remove(hs.stateFile)
		return
	}
	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		slog.Error("heartbeat: persist state failed", "error", err)
		return
	}
	if err := os.WriteFile(hs.stateFile, data, 0o644); err != nil {
		slog.Error("heartbeat: write state file failed", "error", err)
	}
}

// ── ticker loop ──────────────────────────────────────────────

func (hs *HeartbeatScheduler) run(entry *heartbeatEntry) {
	for {
		select {
		case <-hs.stopCh:
			return
		case <-entry.stopCh:
			return
		case <-entry.ticker.C:
			hs.mu.Lock()
			paused := entry.paused
			stopped := hs.stopped
			var exec heartbeatExecution
			if !paused && !stopped {
				exec = heartbeatExecution{
					entry:   entry,
					project: entry.project,
					config:  entry.config,
					engine:  entry.engine,
					workDir: entry.workDir,
				}
				hs.inflight.Add(1)
			}
			hs.mu.Unlock()
			if !paused && !stopped {
				hs.execute(exec)
				hs.inflight.Done()
			}
		}
	}
}

func (hs *HeartbeatScheduler) execute(exec heartbeatExecution) {
	cfg := exec.config

	prompt := cfg.Prompt
	if prompt == "" {
		prompt = readHeartbeatMD(exec.workDir)
	}
	if prompt == "" {
		prompt = defaultHeartbeatPrompt
	}

	slog.Info("heartbeat: executing", "project", exec.project, "session_key", cfg.SessionKey, "prompt_len", len(prompt))

	timeout := time.Duration(cfg.TimeoutMins) * time.Minute
	ctx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	var err error
	if cfg.OnlyWhenIdle {
		err = exec.engine.ExecuteHeartbeatOnlyIfIdleContext(ctx, cfg.SessionKey, prompt, cfg.Silent)
	} else {
		err = exec.engine.ExecuteHeartbeatContext(ctx, cfg.SessionKey, prompt, cfg.Silent)
	}
	cancel()

	hs.mu.Lock()
	exec.entry.runCount++
	exec.entry.lastRun = time.Now()
	if errors.Is(err, errProjectSchedulerBusy) {
		exec.entry.skippedBusy++
		exec.entry.lastError = ""
		slog.Debug("heartbeat: project busy, skipping", "project", exec.project, "session_key", cfg.SessionKey)
	} else if err != nil {
		exec.entry.errorCount++
		exec.entry.lastError = err.Error()
		slog.Error("heartbeat: execution failed", "project", exec.project, "error", err)
	} else {
		exec.entry.lastError = ""
		slog.Info("heartbeat: execution completed", "project", exec.project)
	}
	hs.mu.Unlock()
}

const defaultHeartbeatPrompt = `This is a periodic heartbeat check. Please briefly review:
- Any pending tasks or unfinished work
- Current project status
If nothing needs attention, respond briefly that all is well.`

func readHeartbeatMD(workDir string) string {
	if workDir == "" {
		return ""
	}
	candidates := []string{
		filepath.Join(workDir, "HEARTBEAT.md"),
		filepath.Join(workDir, "heartbeat.md"),
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err == nil {
			content := strings.TrimSpace(string(data))
			if content != "" {
				slog.Debug("heartbeat: loaded prompt from file", "path", path)
				return content
			}
		}
	}
	return ""
}
