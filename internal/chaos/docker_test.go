package chaos

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeRunner answers by the docker subcommand ("inspect", "ps", "network", "compose", ...).
type fakeRunner struct {
	out   map[string]string
	calls [][]string
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	if len(args) == 0 {
		return "", nil
	}
	return f.out[args[0]], nil
}

func (f *fakeRunner) called(sub string) bool {
	for _, c := range f.calls {
		if len(c) > 1 && c[1] == sub {
			return true
		}
	}
	return false
}

func (f *fakeRunner) callContaining(parts ...string) bool {
	for _, c := range f.calls {
		joined := " " + strings.Join(c, " ") + " "
		ok := true
		for _, p := range parts {
			if !strings.Contains(joined, " "+p+" ") {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func TestRefusesContainerOutsideProject(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "\n"}}
	d := &Docker{R: f, Project: "kafkalab"}
	err := d.Stop(context.Background(), "timescaledb")
	if !errors.Is(err, ErrNotLab) {
		t.Fatalf("want ErrNotLab, got %v", err)
	}
	if f.called("stop") {
		t.Fatal("stop must not run")
	}
}

func TestStopLabContainer(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "kafkalab\n"}}
	var seen string
	d := &Docker{R: f, Project: "kafkalab", OnCommand: func(c string) { seen = c }}
	if err := d.Stop(context.Background(), "lab-timescaledb"); err != nil {
		t.Fatal(err)
	}
	if seen != "docker stop -- lab-timescaledb" {
		t.Fatalf("got %q", seen)
	}
}

func TestDisconnectIsGuardedToo(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "otherproject\n"}}
	d := &Docker{R: f, Project: "kafkalab"}
	if err := d.Disconnect(context.Background(), "lab-kafka", "postgres"); !errors.Is(err, ErrNotLab) {
		t.Fatalf("want ErrNotLab, got %v", err)
	}
	if f.called("network") {
		t.Fatal("network command must not run")
	}
}

func TestRecreateSvcWritesEnvAndStopsExtras(t *testing.T) {
	dir := t.TempDir()
	f := &fakeRunner{}
	d := &Docker{R: f, Dir: dir, Project: "kafkalab"}
	if err := d.RecreateSvc(context.Background(), SvcEnv{GroupID: "gap-lab", DLQ: true}, 2); err != nil {
		t.Fatal(err)
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if string(env) != "SVC_GROUP_ID=gap-lab\nSVC_DLQ_ENABLED=true\nSVC_EXTRA_OPTS=\n" {
		t.Fatalf("env %q", env)
	}
	if !f.callContaining("-p", "kafkalab", "up", "-d", "--force-recreate", "gap-svc-1", "gap-svc-2") {
		t.Fatalf("no recreate call: %v", f.calls)
	}
	if !f.callContaining("stop", "gap-svc-3", "gap-svc-4") {
		t.Fatalf("no stop call for extras: %v", f.calls)
	}
}

func TestStatesParsesPs(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"ps": "lab-kafka-1\trunning\nlab-timescaledb\texited\n"}}
	got, err := (&Docker{R: f, Project: "kafkalab"}).States(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"lab-kafka-1": "running", "lab-timescaledb": "exited"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestIPsParsesNetworkInspect(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"network": "lab-gap-svc-1 172.20.0.7/16\nlab-kafka-1 172.20.0.3/16\n"}}
	got, err := (&Docker{R: f, Project: "kafkalab"}).IPs(context.Background(), "lab-kafka")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"172.20.0.7": "lab-gap-svc-1", "172.20.0.3": "lab-kafka-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestGuardInspectsContainersOnlyAndEndsFlags(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "kafkalab\n"}}
	d := &Docker{R: f, Project: "kafkalab"}
	if err := d.Stop(context.Background(), "lab-kafka-1"); err != nil {
		t.Fatal(err)
	}
	if !f.callContaining("inspect", "--type", "container", "--", "lab-kafka-1") || !f.callContaining("stop", "--", "lab-kafka-1") {
		t.Fatalf("%v", f.calls)
	}
}
