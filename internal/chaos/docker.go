// Package chaos drives fault injection through the docker CLI, limited to the lab's compose project.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Runner runs a command and returns its combined output.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) (string, error)
}

// ExecRunner runs real processes.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// ErrNotLab protects the user's other containers (they run a real "timescaledb" too).
var ErrNotLab = errors.New("refusing to touch a container outside compose project kafkalab")

// SvcEnv is what a scenario may change about the gap service; compose reads it from .env.
type SvcEnv struct {
	GroupID   string
	DLQ       bool
	ExtraOpts string
}

func DefaultSvcEnv() SvcEnv { return SvcEnv{GroupID: "gap-lab"} }

func (e SvcEnv) DotEnv() string {
	return fmt.Sprintf("SVC_GROUP_ID=%s\nSVC_DLQ_ENABLED=%t\nSVC_EXTRA_OPTS=%s\n", e.GroupID, e.DLQ, e.ExtraOpts)
}

// MaxSvc is the number of gap-svc-N services defined in compose.yaml.
const MaxSvc = 4

type Docker struct {
	R         Runner
	Dir       string
	Project   string
	OnCommand func(cmd string)
}

func New(dir string) *Docker {
	return &Docker{R: ExecRunner{}, Dir: dir, Project: "kafkalab"}
}

func (d *Docker) run(ctx context.Context, args ...string) (string, error) {
	if d.OnCommand != nil {
		d.OnCommand("docker " + strings.Join(args, " "))
	}
	return d.R.Run(ctx, "docker", args...)
}

func (d *Docker) guard(ctx context.Context, container string) error {
	// --type container stops a same-named image or volume from answering; -- stops a name that looks like a flag.
	out, err := d.R.Run(ctx, "docker", "inspect", "--type", "container", "-f",
		`{{index .Config.Labels "com.docker.compose.project"}}`, "--", container)
	if err != nil {
		return fmt.Errorf("%w: %s (%v)", ErrNotLab, container, err)
	}
	if strings.TrimSpace(out) != d.Project {
		return fmt.Errorf("%w: %s", ErrNotLab, container)
	}
	return nil
}

func (d *Docker) container(ctx context.Context, verb, container string) error {
	if err := d.guard(ctx, container); err != nil {
		return err
	}
	_, err := d.run(ctx, verb, "--", container)
	return err
}

func (d *Docker) Stop(ctx context.Context, c string) error    { return d.container(ctx, "stop", c) }
func (d *Docker) Start(ctx context.Context, c string) error   { return d.container(ctx, "start", c) }
func (d *Docker) Kill(ctx context.Context, c string) error    { return d.container(ctx, "kill", c) }
func (d *Docker) Pause(ctx context.Context, c string) error   { return d.container(ctx, "pause", c) }
func (d *Docker) Unpause(ctx context.Context, c string) error { return d.container(ctx, "unpause", c) }

func (d *Docker) Disconnect(ctx context.Context, network, c string) error {
	if err := d.guard(ctx, c); err != nil {
		return err
	}
	_, err := d.run(ctx, "network", "disconnect", "--", network, c)
	return err
}

func (d *Docker) Connect(ctx context.Context, network, c string) error {
	if err := d.guard(ctx, c); err != nil {
		return err
	}
	_, err := d.run(ctx, "network", "connect", "--", network, c)
	return err
}

// States maps every lab container to its docker state (running, exited, paused, ...).
func (d *Docker) States(ctx context.Context) (map[string]string, error) {
	out, err := d.R.Run(ctx, "docker", "ps", "-a", "--filter", "label=com.docker.compose.project="+d.Project,
		"--format", "{{.Names}}\t{{.State}}")
	if err != nil {
		return nil, err
	}
	states := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		name, state, ok := strings.Cut(line, "\t")
		if ok {
			states[name] = state
		}
	}
	return states, nil
}

// IPs maps container IPs on a network to container names, to tell which instance owns a partition.
func (d *Docker) IPs(ctx context.Context, network string) (map[string]string, error) {
	out, err := d.R.Run(ctx, "docker", "network", "inspect", network, "-f",
		`{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}`)
	if err != nil {
		return nil, err
	}
	ips := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		name, addr, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		ip, _, _ := strings.Cut(addr, "/")
		ips[ip] = name
	}
	return ips, nil
}

func (d *Docker) compose(ctx context.Context, args ...string) error {
	base := []string{"compose", "-p", d.Project, "-f", filepath.Join(d.Dir, "compose.yaml"), "--project-directory", d.Dir}
	_, err := d.run(ctx, append(base, args...)...)
	return err
}

// ComposeUp starts services without recreating them.
func (d *Docker) ComposeUp(ctx context.Context, services ...string) error {
	return d.compose(ctx, append([]string{"up", "-d"}, services...)...)
}

func (d *Docker) StopSvc(ctx context.Context, services ...string) error {
	return d.compose(ctx, append([]string{"stop"}, services...)...)
}

// RecreateSvc runs exactly n service instances with env; recreating is what applies a changed env.
func (d *Docker) RecreateSvc(ctx context.Context, env SvcEnv, n int) error {
	if err := os.WriteFile(filepath.Join(d.Dir, ".env"), []byte(env.DotEnv()), 0o644); err != nil {
		return err
	}
	var up, extra []string
	for i := 1; i <= MaxSvc; i++ {
		if i <= n {
			up = append(up, fmt.Sprintf("gap-svc-%d", i))
		} else {
			extra = append(extra, fmt.Sprintf("gap-svc-%d", i))
		}
	}
	if len(extra) > 0 {
		if err := d.StopSvc(ctx, extra...); err != nil {
			return err
		}
	}
	if len(up) == 0 {
		return nil
	}
	return d.compose(ctx, append([]string{"up", "-d", "--force-recreate"}, up...)...)
}
