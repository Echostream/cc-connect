//go:build windows

package core

import (
	"os/exec"
	"strconv"
)

func prepareWebhookShellCommand(cmd *exec.Cmd, token string) {}

func terminateWebhookShellCommand(cmd *exec.Cmd, token string) {
	if cmd.Process == nil {
		return
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	_ = exec.Command("taskkill", "/T", "/F", "/PID", pid).Run()
	_ = cmd.Process.Kill()
}
