package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/config"
	"github.com/chenhg5/cc-connect/core"
)

type stubMainAgent struct {
	workDir string
}

func (a *stubMainAgent) Name() string { return "stub-main" }

func (a *stubMainAgent) StartSession(_ context.Context, _ string) (core.AgentSession, error) {
	return &stubMainAgentSession{}, nil
}

func (a *stubMainAgent) ListSessions(_ context.Context) ([]core.AgentSessionInfo, error) {
	return nil, nil
}

func (a *stubMainAgent) Stop() error { return nil }

func (a *stubMainAgent) SetWorkDir(dir string) {
	a.workDir = dir
}

func (a *stubMainAgent) GetWorkDir() string {
	return a.workDir
}

type stubMainAgentSession struct{}

func (s *stubMainAgentSession) Send(string, string, []core.ImageAttachment, []core.FileAttachment) error {
	return nil
}
func (s *stubMainAgentSession) RespondPermission(string, core.PermissionResult) error { return nil }
func (s *stubMainAgentSession) Events() <-chan core.Event                             { return nil }
func (s *stubMainAgentSession) Close() error                                          { return nil }
func (s *stubMainAgentSession) CurrentSessionID() string                              { return "" }
func (s *stubMainAgentSession) Alive() bool                                           { return true }

func TestProjectStatePath(t *testing.T) {
	dataDir := t.TempDir()
	got := projectStatePath(dataDir, "my/project:one")
	want := filepath.Join(dataDir, "projects", "my_project_one.state.json")
	if got != want {
		t.Fatalf("projectStatePath() = %q, want %q", got, want)
	}
}

func TestResolveResetOnIdle(t *testing.T) {
	intPtr := func(v int) *int { return &v }

	cases := []struct {
		name          string
		configured    *int
		wantDuration  time.Duration
		wantDefaulted bool
	}{
		{
			name:          "unset applies default and reports defaulted",
			configured:    nil,
			wantDuration:  time.Duration(defaultResetOnIdleMins) * time.Minute,
			wantDefaulted: true,
		},
		{
			name:          "explicit zero opts out and is not defaulted",
			configured:    intPtr(0),
			wantDuration:  0,
			wantDefaulted: false,
		},
		{
			name:          "explicit positive value is honored",
			configured:    intPtr(45),
			wantDuration:  45 * time.Minute,
			wantDefaulted: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotDuration, gotDefaulted := resolveResetOnIdle(tc.configured)
			if gotDuration != tc.wantDuration {
				t.Errorf("duration = %v, want %v", gotDuration, tc.wantDuration)
			}
			if gotDefaulted != tc.wantDefaulted {
				t.Errorf("defaulted = %v, want %v", gotDefaulted, tc.wantDefaulted)
			}
		})
	}
}

func TestApplyProjectStateOverride(t *testing.T) {
	baseDir := t.TempDir()
	overrideDir := filepath.Join(t.TempDir(), "override")
	if err := os.Mkdir(overrideDir, 0o755); err != nil {
		t.Fatalf("mkdir override dir: %v", err)
	}

	store := core.NewProjectStateStore(filepath.Join(t.TempDir(), "projects", "demo.state.json"))
	store.SetWorkDirOverride(overrideDir)

	agent := &stubMainAgent{workDir: baseDir}
	got := applyProjectStateOverride("demo", agent, baseDir, store)

	if got != overrideDir {
		t.Fatalf("applyProjectStateOverride() = %q, want %q", got, overrideDir)
	}
	if agent.workDir != overrideDir {
		t.Fatalf("agent workDir = %q, want %q", agent.workDir, overrideDir)
	}
}

type stubProviderRefreshAgent struct {
	stubMainAgent
	providers  []core.ProviderConfig
	activeName string
	calls      []string
	activateOK bool
}

func (a *stubProviderRefreshAgent) SetProviders(providers []core.ProviderConfig) {
	a.providers = append([]core.ProviderConfig(nil), providers...)
	a.calls = append(a.calls, "set_providers")
}

func (a *stubProviderRefreshAgent) SetActiveProvider(name string) bool {
	if !a.activateOK {
		a.calls = append(a.calls, "set_active_provider_failed")
		return false
	}
	a.activeName = name
	a.calls = append(a.calls, "set_active_provider")
	return true
}

func (a *stubProviderRefreshAgent) GetActiveProvider() *core.ProviderConfig {
	for i := range a.providers {
		if a.providers[i].Name == a.activeName {
			return &a.providers[i]
		}
	}
	return nil
}

func (a *stubProviderRefreshAgent) ListProviders() []core.ProviderConfig {
	providers := make([]core.ProviderConfig, len(a.providers))
	copy(providers, a.providers)
	return providers
}

func (a *stubProviderRefreshAgent) StartInitialModelRefresh() {
	a.calls = append(a.calls, "start_initial_model_refresh")
}

func TestBuildAgentOptionsInjectsProjectScope(t *testing.T) {
	proj := config.ProjectConfig{
		Name: "demo-project",
		Agent: config.AgentConfig{
			Options: map[string]any{
				"work_dir": "/tmp/work",
				"model":    "gpt-test",
			},
		},
	}

	got := buildAgentOptions("/tmp/data", proj)

	if got["cc_data_dir"] != "/tmp/data" {
		t.Fatalf("cc_data_dir = %v, want %q", got["cc_data_dir"], "/tmp/data")
	}
	if got["cc_project"] != "demo-project" {
		t.Fatalf("cc_project = %v, want %q", got["cc_project"], "demo-project")
	}
	if got["work_dir"] != "/tmp/work" || got["model"] != "gpt-test" {
		t.Fatalf("buildAgentOptions() lost existing options: %v", got)
	}
	if _, exists := proj.Agent.Options["cc_data_dir"]; exists {
		t.Fatalf("project agent options mutated: %v", proj.Agent.Options)
	}
}

func TestWireAgentProvidersStartsRefreshAfterProviderWiring(t *testing.T) {
	agent := &stubProviderRefreshAgent{activateOK: true}
	proj := config.ProjectConfig{
		Agent: config.AgentConfig{
			Options: map[string]any{"provider": "provider-b"},
			Providers: []config.ProviderConfig{
				{Name: "provider-a", APIKey: "key-a"},
				{Name: "provider-b", APIKey: "key-b", Model: "model-b"},
			},
		},
	}

	result := wireAgentProviders(agent, proj.Agent)
	startInitialRefreshIfReady(agent, result)

	wantCalls := []string{"set_providers", "set_active_provider", "start_initial_model_refresh"}
	if !reflect.DeepEqual(agent.calls, wantCalls) {
		t.Fatalf("call order = %v, want %v", agent.calls, wantCalls)
	}
	if len(agent.providers) != 2 {
		t.Fatalf("provider count = %d, want 2", len(agent.providers))
	}
	if agent.activeName != "provider-b" {
		t.Fatalf("active provider = %q, want %q", agent.activeName, "provider-b")
	}
}

func TestWireAgentProviders_SkipsRefreshWhenExplicitProviderActivationFails(t *testing.T) {
	agent := &stubProviderRefreshAgent{}
	agent.activateOK = false
	agent.workDir = "/tmp/original"
	proj := config.ProjectConfig{
		Agent: config.AgentConfig{
			Options:   map[string]any{"provider": "missing-provider"},
			Providers: []config.ProviderConfig{{Name: "provider-a", APIKey: "key-a"}},
		},
	}

	result := wireAgentProviders(agent, proj.Agent)

	if result.canStartInitialRefresh {
		t.Fatal("canStartInitialRefresh = true, want false")
	}
	if !result.explicitProviderRequested {
		t.Fatal("explicitProviderRequested = false, want true")
	}
	if result.activeProviderApplied {
		t.Fatal("activeProviderApplied = true, want false")
	}
	wantCalls := []string{"set_providers", "set_active_provider_failed"}
	if !reflect.DeepEqual(agent.calls, wantCalls) {
		t.Fatalf("call order = %v, want %v", agent.calls, wantCalls)
	}
}

func TestWireAgentProviders_AllowsRefreshWithoutProviders(t *testing.T) {
	agent := &stubProviderRefreshAgent{stubMainAgent: stubMainAgent{workDir: "/tmp/original"}}
	proj := config.ProjectConfig{Agent: config.AgentConfig{Options: map[string]any{}}}

	result := wireAgentProviders(agent, proj.Agent)

	if !result.canStartInitialRefresh {
		t.Fatal("canStartInitialRefresh = false, want true")
	}
	if result.explicitProviderRequested {
		t.Fatal("explicitProviderRequested = true, want false")
	}
	if result.activeProviderApplied {
		t.Fatal("activeProviderApplied = true, want false")
	}
	if len(agent.calls) != 0 {
		t.Fatalf("calls = %v, want no provider wiring calls", agent.calls)
	}
}

func TestStartInitialRefresh_AfterProjectStateOverride(t *testing.T) {
	agent := &stubProviderRefreshAgent{activateOK: true, stubMainAgent: stubMainAgent{workDir: "/tmp/original"}}
	overrideDir := filepath.Join(t.TempDir(), "override")
	if err := os.Mkdir(overrideDir, 0o755); err != nil {
		t.Fatalf("mkdir override dir: %v", err)
	}
	store := core.NewProjectStateStore(filepath.Join(t.TempDir(), "projects", "demo.state.json"))
	store.SetWorkDirOverride(overrideDir)
	proj := config.ProjectConfig{
		Name: "demo",
		Agent: config.AgentConfig{
			Options:   map[string]any{"provider": "provider-b", "work_dir": "/tmp/original"},
			Providers: []config.ProviderConfig{{Name: "provider-a"}, {Name: "provider-b"}},
		},
	}

	result := wireAgentProviders(agent, proj.Agent)
	applyProjectStateOverride(proj.Name, agent, "/tmp/original", store)
	startInitialRefreshIfReady(agent, result)

	wantCalls := []string{"set_providers", "set_active_provider", "start_initial_model_refresh"}
	if !reflect.DeepEqual(agent.calls, wantCalls) {
		t.Fatalf("call order = %v, want %v", agent.calls, wantCalls)
	}
	if agent.workDir != overrideDir {
		t.Fatalf("agent workDir at refresh = %q, want %q", agent.workDir, overrideDir)
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = old
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy stderr: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return buf.String()
}

func TestPrintUsage_ListsCronExecCommand(t *testing.T) {
	out := captureStderr(t, printUsage)

	if !strings.Contains(out, "Manage scheduled tasks") {
		t.Fatalf("printUsage() output missing cron section:\n%s", out)
	}
	if !strings.Contains(out, "exec             Trigger a scheduled task immediately") {
		t.Fatalf("printUsage() output missing cron exec command:\n%s", out)
	}
}

func TestCanonicalCronSubcommand_ManualTriggerAliases(t *testing.T) {
	for _, sub := range []string{"exec", "run", "trigger"} {
		if got := canonicalCronSubcommand(sub); got != "exec" {
			t.Fatalf("canonicalCronSubcommand(%q) = %q, want exec", sub, got)
		}
	}
}

func TestValidateNoExtraTopLevelArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name: "no extra args",
			args: nil,
		},
		{
			name:    "unknown command",
			args:    []string{"bind", "--help"},
			wantErr: "unknown top-level command: bind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNoExtraTopLevelArgs(tt.args)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateNoExtraTopLevelArgs(%v) error = %v, want nil", tt.args, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateNoExtraTopLevelArgs(%v) error = nil, want %q", tt.args, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateNoExtraTopLevelArgs(%v) error = %q, want substring %q", tt.args, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestParseRootCLIOptionsGlobalFlagsBeforeSubcommand(t *testing.T) {
	opts, err := parseRootCLIOptions([]string{"--config", "/tmp/test-config.toml", "--log-max-size", "12MB", "--log-max-backups", "7", "sessions", "list"})
	if err != nil {
		t.Fatalf("parseRootCLIOptions() error = %v", err)
	}
	if opts.configPath != "/tmp/test-config.toml" {
		t.Fatalf("configPath = %q, want %q", opts.configPath, "/tmp/test-config.toml")
	}
	if opts.logMaxSize != "12MB" {
		t.Fatalf("logMaxSize = %q, want %q", opts.logMaxSize, "12MB")
	}
	if opts.logMaxBackups != 7 {
		t.Fatalf("logMaxBackups = %d, want 7", opts.logMaxBackups)
	}
	if opts.showVersion {
		t.Fatal("showVersion = true, want false")
	}
	wantArgs := []string{"sessions", "list"}
	if !reflect.DeepEqual(opts.args, wantArgs) {
		t.Fatalf("args = %v, want %v", opts.args, wantArgs)
	}
}

func TestTopLevelCommandHandlersIncludeTimerAliases(t *testing.T) {
	for _, command := range []string{"timer", "at"} {
		if topLevelCommandHandlers[command] == nil {
			t.Fatalf("topLevelCommandHandlers[%q] is nil", command)
		}
	}
}

func TestParseRootCLIOptionsPreservesSubcommandHelp(t *testing.T) {
	opts, err := parseRootCLIOptions([]string{"--config", "/tmp/test-config.toml", "send", "--help"})
	if err != nil {
		t.Fatalf("parseRootCLIOptions() error = %v", err)
	}
	wantArgs := []string{"send", "--help"}
	if !reflect.DeepEqual(opts.args, wantArgs) {
		t.Fatalf("args = %v, want %v", opts.args, wantArgs)
	}
}

func TestParseRootCLIOptionsHelp(t *testing.T) {
	_, err := parseRootCLIOptions([]string{"--help"})
	if err == nil {
		t.Fatal("parseRootCLIOptions(--help) error = nil, want flag.ErrHelp")
	}
	if !strings.Contains(err.Error(), flag.ErrHelp.Error()) {
		t.Fatalf("parseRootCLIOptions(--help) error = %q, want %q", err.Error(), flag.ErrHelp.Error())
	}
}

func TestRunTopLevelCommandUnknown(t *testing.T) {
	if runTopLevelCommand([]string{"bind", "--help"}) {
		t.Fatal("runTopLevelCommand() handled unknown command")
	}
}

type shutdownOrderPlatform struct {
	name  string
	order *[]string
}

func (p *shutdownOrderPlatform) Name() string                             { return p.name }
func (p *shutdownOrderPlatform) Start(core.MessageHandler) error          { return nil }
func (p *shutdownOrderPlatform) Reply(context.Context, any, string) error { return nil }
func (p *shutdownOrderPlatform) Send(context.Context, any, string) error  { return nil }
func (p *shutdownOrderPlatform) Stop() error {
	*p.order = append(*p.order, p.name)
	return nil
}

func TestBuildSchedulerRuntimePlan(t *testing.T) {
	intPtr := func(value int) *int { return &value }

	tests := []struct {
		name              string
		cfg               *config.Config
		proj              *config.ProjectConfig
		wantType          config.SchedulerType
		wantMaxWaiting    int
		wantWaitingTTL    time.Duration
		wantRunTimeout    time.Duration
		wantLegacyMaxTurn time.Duration
	}{
		{
			name:           "missing scheduler defaults to legacy",
			cfg:            &config.Config{},
			proj:           &config.ProjectConfig{},
			wantType:       config.SchedulerTypeLegacy,
			wantMaxWaiting: 5,
		},
		{
			name: "explicit legacy uses positive global queue depth",
			cfg: &config.Config{
				Queue: config.QueueConfig{MaxDepth: intPtr(2)},
			},
			proj: &config.ProjectConfig{
				Scheduler: &config.SchedulerConfig{Type: config.SchedulerTypeLegacy},
			},
			wantType:       config.SchedulerTypeLegacy,
			wantMaxWaiting: 2,
		},
		{
			name: "legacy zero queue and max turn preserve disabled defaults",
			cfg: &config.Config{
				Queue:           config.QueueConfig{MaxDepth: intPtr(0)},
				MaxTurnTimeMins: intPtr(0),
			},
			proj:           &config.ProjectConfig{},
			wantType:       config.SchedulerTypeLegacy,
			wantMaxWaiting: 5,
		},
		{
			name: "legacy positive max turn is converted to minutes",
			cfg: &config.Config{
				MaxTurnTimeMins: intPtr(15),
			},
			proj:              &config.ProjectConfig{},
			wantType:          config.SchedulerTypeLegacy,
			wantMaxWaiting:    5,
			wantLegacyMaxTurn: 15 * time.Minute,
		},
		{
			name: "project omitted numerics use project defaults",
			cfg:  &config.Config{},
			proj: &config.ProjectConfig{
				Scheduler: &config.SchedulerConfig{Type: config.SchedulerTypeProject},
			},
			wantType:       config.SchedulerTypeProject,
			wantMaxWaiting: 10,
			wantWaitingTTL: 6 * time.Hour,
			wantRunTimeout: 30 * time.Minute,
		},
		{
			name: "project explicit numerics are converted to minutes",
			cfg:  &config.Config{},
			proj: &config.ProjectConfig{
				Scheduler: &config.SchedulerConfig{
					Type:            config.SchedulerTypeProject,
					MaxWaiting:      intPtr(11),
					WaitingTTLMins:  intPtr(361),
					MaxTurnTimeMins: intPtr(31),
				},
			},
			wantType:       config.SchedulerTypeProject,
			wantMaxWaiting: 11,
			wantWaitingTTL: 361 * time.Minute,
			wantRunTimeout: 31 * time.Minute,
		},
		{
			name: "project ignores legacy global queue and max turn",
			cfg: &config.Config{
				Queue:           config.QueueConfig{MaxDepth: intPtr(2)},
				MaxTurnTimeMins: intPtr(15),
			},
			proj: &config.ProjectConfig{
				Scheduler: &config.SchedulerConfig{Type: config.SchedulerTypeProject},
			},
			wantType:       config.SchedulerTypeProject,
			wantMaxWaiting: 10,
			wantWaitingTTL: 6 * time.Hour,
			wantRunTimeout: 30 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildSchedulerRuntimePlan(tc.cfg, tc.proj)
			if err != nil {
				t.Fatalf("buildSchedulerRuntimePlan() error = %v", err)
			}
			if got.schedulerType != tc.wantType ||
				got.maxWaiting != tc.wantMaxWaiting ||
				got.waitingTTL != tc.wantWaitingTTL ||
				got.runTimeout != tc.wantRunTimeout ||
				got.legacyMaxTurnTime != tc.wantLegacyMaxTurn {
				t.Fatalf("plan = %+v, want type=%q maxWaiting=%d waitingTTL=%v runTimeout=%v legacyMaxTurn=%v",
					got, tc.wantType, tc.wantMaxWaiting, tc.wantWaitingTTL, tc.wantRunTimeout, tc.wantLegacyMaxTurn)
			}
		})
	}
}

func TestBuildSchedulerRuntimePlansKeepsProjectsIndependent(t *testing.T) {
	projectType := config.SchedulerTypeProject
	cfg := &config.Config{
		Projects: []config.ProjectConfig{
			{Name: "legacy"},
			{Name: "project", Scheduler: &config.SchedulerConfig{Type: projectType}},
		},
	}

	plans, err := buildSchedulerRuntimePlans(cfg)
	if err != nil {
		t.Fatalf("buildSchedulerRuntimePlans() error = %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("plan count = %d, want 2", len(plans))
	}
	if plans[0].schedulerType != config.SchedulerTypeLegacy || plans[0].maxWaiting != 5 {
		t.Fatalf("legacy plan = %+v", plans[0])
	}
	if plans[1].schedulerType != config.SchedulerTypeProject || plans[1].maxWaiting != 10 || plans[1].waitingTTL != 6*time.Hour || plans[1].runTimeout != 30*time.Minute {
		t.Fatalf("project plan = %+v", plans[1])
	}
}

func TestBuildSchedulerRuntimePlanRejectsInvalidScheduler(t *testing.T) {
	_, err := buildSchedulerRuntimePlan(&config.Config{}, &config.ProjectConfig{
		Scheduler: &config.SchedulerConfig{},
	})
	if err == nil {
		t.Fatal("buildSchedulerRuntimePlan() error = nil, want invalid scheduler error")
	}
}

func TestSchedulerRuntimePlanConstructsSelectedScheduler(t *testing.T) {
	legacyPlan := schedulerRuntimePlan{schedulerType: config.SchedulerTypeLegacy, maxWaiting: 5}
	legacyScheduler := legacyPlan.newScheduler()
	defer legacyScheduler.Stop(context.Background())
	if _, ok := legacyScheduler.(*core.LegacyScheduler); !ok {
		t.Fatalf("legacy scheduler type = %T", legacyScheduler)
	}

	projectPlan := schedulerRuntimePlan{
		schedulerType: config.SchedulerTypeProject,
		maxWaiting:    10,
		waitingTTL:    6 * time.Hour,
		runTimeout:    30 * time.Minute,
	}
	projectScheduler := projectPlan.newScheduler()
	defer projectScheduler.Stop(context.Background())
	if _, ok := projectScheduler.(*core.ProjectScheduler); !ok {
		t.Fatalf("project scheduler type = %T", projectScheduler)
	}
}

type recordingMaxTurnTimeSetter struct {
	calls []time.Duration
}

func (s *recordingMaxTurnTimeSetter) SetMaxTurnTime(timeout time.Duration) {
	s.calls = append(s.calls, timeout)
}

func TestSchedulerRuntimePlanConfiguresOnlyLegacyPositiveMaxTurnTime(t *testing.T) {
	tests := []struct {
		name      string
		plan      schedulerRuntimePlan
		wantCalls []time.Duration
	}{
		{
			name: "legacy positive timeout is applied",
			plan: schedulerRuntimePlan{
				schedulerType:     config.SchedulerTypeLegacy,
				legacyMaxTurnTime: 15 * time.Minute,
			},
			wantCalls: []time.Duration{15 * time.Minute},
		},
		{
			name: "legacy zero timeout remains disabled",
			plan: schedulerRuntimePlan{
				schedulerType: config.SchedulerTypeLegacy,
			},
		},
		{
			name: "project ignores legacy timeout",
			plan: schedulerRuntimePlan{
				schedulerType:     config.SchedulerTypeProject,
				legacyMaxTurnTime: 15 * time.Minute,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setter := &recordingMaxTurnTimeSetter{}
			tc.plan.configureEngine(setter)
			if !reflect.DeepEqual(setter.calls, tc.wantCalls) {
				t.Fatalf("SetMaxTurnTime calls = %v, want %v", setter.calls, tc.wantCalls)
			}
		})
	}
}

func TestStopEnginesForShutdownStopsAllEnginesWithoutSignals(t *testing.T) {
	var order []string
	e1 := core.NewEngine("one", &stubMainAgent{}, []core.Platform{&shutdownOrderPlatform{name: "p1", order: &order}}, filepath.Join(t.TempDir(), "s1.json"), core.LangEnglish)
	e2 := core.NewEngine("two", &stubMainAgent{}, []core.Platform{&shutdownOrderPlatform{name: "p2", order: &order}}, filepath.Join(t.TempDir(), "s2.json"), core.LangEnglish)
	stopEnginesForShutdown([]*core.Engine{e1, nil, e2}, func(err error) { t.Fatalf("unexpected stop error: %v", err) })
	if got, want := strings.Join(order, ","), "p1,p2"; got != want {
		t.Fatalf("shutdown platform stop order = %q, want %q", got, want)
	}
}
