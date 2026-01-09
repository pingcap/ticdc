package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/pdutil"
)

var defaultScenarioOrder = []string{"single-table", "multi-table", "wide-table", "narrow-table"}

type runConfig struct {
	DataDir  string
	Duration time.Duration
}

func main() {
	dataDir := flag.String("data-dir", "./eventstore-bench-data", "directory used to store temporary eventStore data")
	duration := flag.Duration("duration", time.Minute, "per-scenario duration (0 means run until interrupted)")
	scenarioFlag := flag.String("scenarios", strings.Join(defaultScenarioOrder, ","), "comma separated scenario names or 'all'")
	writerOverride := flag.Int("writers-per-table", 0, "override writers per table for every scenario")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scenarios := selectScenarios(*scenarioFlag)
	if len(scenarios) == 0 {
		fmt.Fprintln(os.Stderr, "no benchmarking scenario matched the provided filter")
		os.Exit(1)
	}

	if *writerOverride > 0 {
		for i := range scenarios {
			scenarios[i].WritersPerTable = *writerOverride
		}
	}

	cfg := runConfig{
		DataDir:  *dataDir,
		Duration: *duration,
	}

	results := make([]benchResult, 0, len(scenarios))
	for _, scenario := range scenarios {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("\n=== Running %s ===\n", scenario.formatHeading())
		result, err := runScenario(ctx, scenario, cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "scenario %s failed: %v\n", scenario.Name, err)
			if ctx.Err() != nil {
				break
			}
		}
		fmt.Printf("Duration: %s\n", result.Duration.Round(time.Millisecond))
		fmt.Printf("Throughput: %.2f events/s, %.2f MB/s (total events %d)\n",
			result.ThroughputEvents, bytesToMB(result.ThroughputBytes), result.Events)
		fmt.Printf("Latency avg %s  (p50/p95/p99/max: %s)\n", result.avgLatencySummary(), result.latencySummary())
		results = append(results, result)
	}

	if len(results) > 1 {
		fmt.Println("\nSummary:")
		printSummary(results)
	}
}

func selectScenarios(filter string) []benchScenario {
	definitions := defaultBenchScenarios()
	requested := parseScenarioNames(filter)
	if len(requested) == 0 {
		requested = defaultScenarioOrder
	}
	includeAll := false
	for _, name := range requested {
		if name == "all" {
			includeAll = true
			break
		}
	}
	result := make([]benchScenario, 0, len(definitions))
	ordered := defaultScenarioOrder
	if includeAll {
		for _, name := range ordered {
			if scenario, ok := definitions[name]; ok {
				result = append(result, scenario)
			}
		}
		return result
	}
	seen := make(map[string]struct{})
	for _, name := range requested {
		if scenario, ok := definitions[name]; ok {
			if _, exists := seen[name]; !exists {
				result = append(result, scenario)
				seen[name] = struct{}{}
			}
		}
	}
	return result
}

func runScenario(ctx context.Context, scenario benchScenario, cfg runConfig) (result benchResult, storeErr error) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())

	if err := os.MkdirAll(filepath.Join(cfg.DataDir, scenario.Name), 0o755); err != nil {
		return benchResult{}, err
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	stats := newBenchStats(scenario.Name)
	subClient := newBenchSubscriptionClient(runCtx, scenario, stats)
	store := eventstore.New(filepath.Join(cfg.DataDir, scenario.Name), subClient)

	runErrCh := make(chan error, 1)
	go func() {
		if err := store.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			runErrCh <- err
			return
		}
		runErrCh <- nil
	}()

	defer func() {
		cancel()
		<-runCtx.Done()
		_ = subClient.Close(context.Background())
		_ = store.Close(context.Background())
		if err := <-runErrCh; err != nil {
			storeErr = err
		}
	}()

	changefeedID := common.NewChangefeedID4Test("eventstore-bench", scenario.Name)
	spans := scenario.spanList()
	dispatchers := make([]common.DispatcherID, 0, len(spans))
	for _, span := range spans {
		dispatcherID := common.NewDispatcherID()
		ok := store.RegisterDispatcher(changefeedID, dispatcherID, span, scenario.startTS(), func(uint64, uint64) {}, false, false)
		if !ok {
			return benchResult{}, fmt.Errorf("failed to register dispatcher for table %d", span.TableID)
		}
		dispatchers = append(dispatchers, dispatcherID)
	}

	<-runCtx.Done()

	for _, dispatcherID := range dispatchers {
		store.UnregisterDispatcher(changefeedID, dispatcherID)
	}

	duration := stats.elapsed()
	result = stats.snapshot(scenario, duration)
	return result, storeErr
}

func bytesToMB(value float64) float64 {
	return value / (1024.0 * 1024.0)
}

func printSummary(results []benchResult) {
	fmt.Printf("%-15s %-12s %-12s %-12s %-22s\n", "Scenario", "Events/s", "MB/s", "Avg Lat", "p50/p95/p99/max")
	for _, res := range results {
		fmt.Printf("%-15s %-12.2f %-12.2f %-12s %-22s\n",
			res.Scenario.Name,
			res.ThroughputEvents,
			bytesToMB(res.ThroughputBytes),
			res.avgLatencySummary(),
			res.latencySummary())
	}
}
