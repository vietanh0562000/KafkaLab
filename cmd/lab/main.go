// Command lab serves the Kafka lab dashboard, or runs one scenario headless with -run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5/pgxpool"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
	"kafkalab/internal/scenario"
	"kafkalab/internal/web"
	static "kafkalab/web"
)

const dbURL = "postgres://postgres:postgres@localhost:15432/postgres?connect_timeout=2"

type backend struct {
	lab    *lab.Lab
	runner *scenario.Runner
}

func (b backend) Summaries() []scenario.Summary { return b.runner.Summaries() }
func (b backend) Start(id string) error         { return b.runner.Start(id) }
func (b backend) Reset() error                  { return b.runner.Reset() }
func (b backend) Snapshot() observe.Snapshot    { return b.lab.Snapshot() }
func (b backend) Chaos(ctx context.Context, action, target string) error {
	return b.lab.Chaos(ctx, action, target)
}
func (b backend) Produce(ctx context.Context, r lab.ProduceReq) error {
	res, err := b.lab.Produce(ctx, r)
	failed := 0
	for _, x := range res {
		if x.Err != nil {
			failed++
			err = errors.Join(err, x.Err)
		}
	}
	if err != nil {
		b.lab.Log("produce: %d of %d failed: %v", failed, len(res), err)
	} else {
		b.lab.Log("produce: %d records acknowledged", len(res))
	}
	return err
}

func main() {
	addr := flag.String("addr", ":7070", "listen address")
	dir := flag.String("dir", ".", "directory holding compose.yaml")
	run := flag.String("run", "", "run one scenario headless and exit (0 = pass)")
	flag.Parse()

	absDir, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hub := events.NewHub()
	docker := chaos.New(absDir)
	admin, err := kafkax.NewAdmin()
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()
	pool, err := pgxpool.New(ctx, dbURL) // connects lazily, so a stopped DB does not stop the lab
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	l := lab.New(docker, admin, pool, hub)
	docker.OnCommand = func(cmd string) { l.Log("$ %s", cmd) }
	poller := observe.NewPoller(docker, admin, pool, l.Group)
	l.Snap = poller.Latest
	runner := scenario.NewRunner(l, hub, scenario.All())
	go poller.Run(ctx, time.Second, func(s observe.Snapshot) {
		hub.Publish(events.Event{Type: "snapshot", Data: s})
	})

	if *run != "" {
		os.Exit(headless(ctx, runner, hub, *run))
	}

	srv := &http.Server{Addr: *addr, Handler: web.NewServer(backend{lab: l, runner: runner}, hub, static.Static())}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("Kafka Lab on http://localhost%s", *addr)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}

func headless(ctx context.Context, runner *scenario.Runner, hub *events.Hub, id string) int {
	ch, cancel := hub.Subscribe()
	defer cancel()
	go func() {
		for e := range ch {
			switch d := e.Data.(type) {
			case scenario.StepEvent:
				if d.Status == "running" {
					fmt.Printf("…    %s\n", d.Step)
				}
			case string:
				if e.Type == "log" {
					fmt.Printf("     %s\n", d)
				}
			}
		}
	}()
	time.Sleep(1500 * time.Millisecond) // let the first snapshot land before steps read it
	res, err := runner.RunSync(ctx, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("\n== %s ==\n", id)
	for _, s := range res.Steps {
		status := "PASS"
		if !s.Passed {
			status = "FAIL"
		}
		line := fmt.Sprintf("%s %s (%.1fs)", status, s.Name, s.Took.Seconds())
		if s.Observed != "" {
			line += " — " + s.Observed
		}
		if s.Err != "" {
			line += " — " + s.Err
		}
		fmt.Println(line)
	}
	if res.Passed {
		return 0
	}
	return 1
}
