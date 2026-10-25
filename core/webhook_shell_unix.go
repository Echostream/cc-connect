//go:build !windows

package core

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

const webhookShellTokenEnv = "CC_CONNECT_WEBHOOK_SHELL_TOKEN"

// Webhook shell termination is best-effort containment for portable Unix shells:
// it kills the original process group, the currently visible PPID subtree, and
// descendants that still carry this per-command token after reparenting/setsid.
// A command that fully daemonizes, clears its environment, and closes inherited
// stdio cannot be reliably contained without OS-level mechanisms such as
// cgroups/systemd scopes or a dedicated supervisor.

func prepareWebhookShellCommand(cmd *exec.Cmd, token string) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	if token != "" {
		env := cmd.Env
		if env == nil {
			env = os.Environ()
		}
		cmd.Env = append(env, webhookShellTokenEnv+"="+token)
	}
}

func terminateWebhookShellCommand(cmd *exec.Cmd, token string) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	targets := webhookShellTerminationTargets(pid, token)
	for _, target := range targets {
		_ = syscall.Kill(target, syscall.SIGKILL)
	}
	// prepareWebhookShellCommand starts the shell in a new process group with
	// Pgid=0, so the group id is the shell pid. Use it directly instead of
	// querying Getpgid during cancellation: the shell may already be exiting or
	// reaped while background children are still alive, and a failed lookup would
	// otherwise fall back to killing only the shell process.
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		_ = cmd.Process.Kill()
	}
	for _, target := range targets {
		_ = syscall.Kill(target, syscall.SIGKILL)
	}
}

func webhookShellTerminationTargets(rootPID int, token string) []int {
	seen := make(map[int]struct{})
	var targets []int
	add := func(pid int) {
		if pid <= 0 || pid == rootPID {
			return
		}
		if _, ok := seen[pid]; ok {
			return
		}
		seen[pid] = struct{}{}
		targets = append(targets, pid)
	}
	for _, pid := range webhookShellDescendants(rootPID) {
		add(pid)
	}
	for _, pid := range webhookShellTokenProcesses(token) {
		add(pid)
	}
	return targets
}

func webhookShellDescendants(rootPID int) []int {
	out, err := exec.Command("ps", "-axo", "pid=,ppid=").Output()
	if err != nil {
		return nil
	}
	childrenByParent := make(map[int][]int)
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 || pid == rootPID {
			continue
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil || ppid <= 0 {
			continue
		}
		childrenByParent[ppid] = append(childrenByParent[ppid], pid)
	}
	var descendants []int
	stack := append([]int(nil), childrenByParent[rootPID]...)
	for len(stack) > 0 {
		pid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		descendants = append(descendants, pid)
		stack = append(stack, childrenByParent[pid]...)
	}
	return descendants
}

func webhookShellTokenProcesses(token string) []int {
	if token == "" {
		return nil
	}
	needle := webhookShellTokenEnv + "=" + token
	out, err := exec.Command("ps", "auxeww").Output()
	if err != nil {
		return nil
	}
	var matches []int
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, needle) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[1])
		if err != nil || pid <= 0 {
			continue
		}
		matches = append(matches, pid)
	}
	return matches
}
