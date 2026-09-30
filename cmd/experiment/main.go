// Command experiment runs the M1 replica-placement experiments (A-D) against
// the frozen M0 baseline and prints plain-text result tables. Every run is
// deterministic for a given seed; change flags, not code, to vary it.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"project18/internal/common"
	"project18/internal/experiment"
)

func header(s string) {
	fmt.Printf("\n%s\n%s\n", s, strings.Repeat("=", len(s)))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func main() {
	chunks := flag.Int("chunks", 120, "chunks to allocate in distribution/failure experiments")
	seeds := flag.Int("seeds", 10, "number of seeds (1..N) for Experiment A spread statistics")
	only := flag.String("only", "", "run only one experiment: A, B, C or D (default: all)")
	flag.Parse()

	run := func(name string) bool { return *only == "" || strings.EqualFold(*only, name) }

	if run("A") {
		expA(*chunks, *seeds)
	}
	if run("B") {
		expB()
	}
	if run("C") {
		expC(*chunks)
	}
	if run("D") {
		expD(*chunks)
	}
}

func expA(chunks, seeds int) {
	header("Experiment A: baseline distribution (seed=1 detail, then spread across seeds)")
	fmt.Printf("chunks=%d; replicas/node figures are per-node counts; imbalance = max-min\n\n", chunks)
	fmt.Printf("%-6s %-3s | %-6s %-6s %-8s %-9s %-8s | %s\n", "nodes", "RF", "min", "max", "mean", "imbalance", "stddev", "imbalance over seeds 1.."+fmt.Sprint(seeds)+" (min/mean/max)")
	for _, n := range []int{3, 5, 10} {
		for _, rf := range []int{2, 3} {
			cfg := experiment.DefaultConfig()
			cfg.NodeCount, cfg.ReplicationFactor, cfg.ChunkCount, cfg.Seed = n, rf, chunks, 1
			res, err := experiment.RunDistributionExperiment(cfg)
			must(err)
			d := res.Distribution

			lo, hi, sum := 1<<30, 0, 0
			for seed := int64(1); seed <= int64(seeds); seed++ {
				cfg.Seed = seed
				r, err := experiment.RunDistributionExperiment(cfg)
				must(err)
				im := r.Distribution.Imbalance
				if im < lo {
					lo = im
				}
				if im > hi {
					hi = im
				}
				sum += im
			}
			fmt.Printf("%-6d %-3d | %-6d %-6d %-8.2f %-9d %-8.2f | %d / %.1f / %d\n",
				n, rf, d.Min, d.Max, d.Mean, d.Imbalance, d.StdDev, lo, float64(sum)/float64(seeds), hi)
		}
	}
	fmt.Println("\nseed=1 per-node detail (nodes=10, RF=3):")
	cfg := experiment.DefaultConfig()
	cfg.NodeCount, cfg.ReplicationFactor, cfg.ChunkCount, cfg.Seed = 10, 3, chunks, 1
	res, err := experiment.RunDistributionExperiment(cfg)
	must(err)
	fmt.Println(" ", res.Distribution)
	fmt.Printf("  placement decisions recorded: %d\n", res.PlacementDecisions)
}

func expB() {
	header("Experiment B: uneven cluster conditions")
	cfg := experiment.DefaultConfig()
	cfg.NodeCount, cfg.ReplicationFactor, cfg.ChunkCount, cfg.ChunkPayloadSize, cfg.Seed = 5, 3, 60, 16, 1

	fmt.Println("\nB1: capacity awareness (5 nodes, RF=3, 60 chunks x 16B; CS1,CS2 capacity=64B (4 chunks), others 1MiB)")
	cap, err := experiment.RunCapacityAwarenessExperiment(cfg, 2, 64, 1<<20)
	must(err)
	fmt.Println("  distribution (physically stored):", cap.Distribution)
	ids := cap.OverflowedNodes
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	fmt.Printf("  chunks where a chosen replica could not physically fit: %d of %d; nodes that overflowed: %v\n",
		cap.OverflowedChunkCount, cfg.ChunkCount, ids)
	fmt.Printf("  after 3 heartbeat ticks: chunks whose master-listed replicas include a node that does not hold them: %d of %d (%d phantom replicas); chunks the master considers under-replicated: %d\n",
		cap.PhantomChunks, cfg.ChunkCount, cap.PhantomReplicas, cap.MasterViewUnderReplicated)
	fmt.Printf("  client read of the whole file succeeded: %v %s\n", cap.ClientReadOK, cap.ClientReadError)

	fmt.Println("\nB2: load awareness (CS1 pre-loaded with 500 unrelated chunks, then 60 fresh chunks allocated)")
	load, err := experiment.RunLoadAwarenessExperiment(cfg, 500)
	must(err)
	fmt.Println("  before:", load.DistributionBefore)
	fmt.Println("  after: ", load.DistributionAfter)
	fmt.Printf("  new replicas received by pre-loaded %s: %d (expected share if load-blind: ~%d)\n",
		load.PreloadedNode,
		load.DistributionAfter.PerServer[load.PreloadedNode]-load.DistributionBefore.PerServer[load.PreloadedNode],
		cfg.ChunkCount*cfg.ReplicationFactor/cfg.NodeCount)

	gainedSum := 0
	for seed := int64(1); seed <= 10; seed++ {
		c := cfg
		c.Seed = seed
		r, err := experiment.RunLoadAwarenessExperiment(c, 500)
		must(err)
		gainedSum += r.DistributionAfter.PerServer[r.PreloadedNode] - r.DistributionBefore.PerServer[r.PreloadedNode]
	}
	fmt.Printf("  across seeds 1..10: mean new replicas on the pre-loaded node = %.1f (uniform expectation %d)\n",
		float64(gainedSum)/10, cfg.ChunkCount*cfg.ReplicationFactor/cfg.NodeCount)

	fmt.Println("\nB3: previously failed and recovered node (6 nodes, RF=3, 60 chunks; kill CS1, re-replicate, recover CS1)")
	cfg.NodeCount = 6
	rec, err := experiment.RunRecoveredNodeExperiment(cfg, "CS1", 60)
	must(err)
	fmt.Printf("  CS1 physical chunks before kill=%d, after recovery=%d\n", rec.ChunksOnTargetBefore, rec.ChunksOnTargetAfter)
	fmt.Printf("  chunks over-replicated per master (alive replicas > RF): %d of %d (max replicas seen on one chunk: %d)\n",
		rec.OverReplicatedChunks, rec.ChunkCount, rec.MaxReplicasSeen)
	fmt.Printf("  chunks under-replicated: %d\n", rec.UnderReplicatedChunks)
	fmt.Printf("  of a fresh batch of %d chunks, replicas landing on recovered CS1: %d (uniform expectation ~%d)\n",
		rec.NewBatchSize, rec.NewChunksGainedByTarget, rec.NewBatchSize*cfg.ReplicationFactor/cfg.NodeCount)
}

func printRound(r experiment.FailureImpactResult) {
	fmt.Printf("  %-4s | affected=%-3d underRepl(peak)=%-3d reRepl start/ok/fail=%d/%d/%d | detect=%v (%d ticks) restored-to-RF=%v (%d ticks, achieved=%v) finalUnder(master view)=%d\n",
		r.Target, r.AffectedChunks, r.UnderReplicatedPeak,
		r.ReReplicationStarted, r.ReReplicationOK, r.ReReplicationFailed,
		r.DetectionDuration, r.DetectionTicks, r.RecoveryDuration, r.RecoveryTicks, r.RecoveryAchieved, r.FinalUnderReplicated)
	fmt.Printf("       pre : %s\n       post: %s\n", r.PreDistribution, r.PostDistribution)
}

func expC(chunks int) {
	header("Experiment C: targeted failure (10 nodes, RF=3)")
	cfg := experiment.DefaultConfig()
	cfg.NodeCount, cfg.ReplicationFactor, cfg.ChunkCount, cfg.Seed = 10, 3, chunks, 1

	// Find most- and least-loaded nodes from the seed-1 baseline, then kill each.
	base, err := experiment.RunDistributionExperiment(cfg)
	must(err)
	var heavy, light common.ServerID
	hi, lo := -1, 1<<30
	ids := make([]common.ServerID, 0)
	for id := range base.Distribution.PerServer {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		n := base.Distribution.PerServer[id]
		if n > hi {
			hi, heavy = n, id
		}
		if n < lo {
			lo, light = n, id
		}
	}
	fmt.Printf("baseline: %s\nheaviest=%s (%d replicas), lightest=%s (%d replicas)\n\n", base.Distribution, heavy, hi, light, lo)

	for _, target := range []common.ServerID{heavy, light} {
		r, err := experiment.RunTargetedFailureExperiment(cfg, target)
		must(err)
		printRound(r)
	}

	hs, ls := 0, 0
	for seed := int64(1); seed <= 10; seed++ {
		c := cfg
		c.Seed = seed
		b, err := experiment.RunDistributionExperiment(c)
		must(err)
		hmax, lmin := -1, 1<<30
		for _, n := range b.Distribution.PerServer {
			if n > hmax {
				hmax = n
			}
			if n < lmin {
				lmin = n
			}
		}
		hs += hmax
		ls += lmin
	}
	fmt.Printf("\nreplicas lost if the heaviest / lightest node dies, mean over seeds 1..10: %.1f / %.1f\n", float64(hs)/10, float64(ls)/10)
}

func expD(chunks int) {
	header("Experiment D: cascading failures (10 nodes, RF=3; no node is ever recovered)")
	cfg := experiment.DefaultConfig()
	cfg.NodeCount, cfg.ReplicationFactor, cfg.ChunkCount, cfg.Seed = 10, 3, chunks, 1

	for _, order := range [][]common.ServerID{
		{"CS2", "CS4", "CS1"},
		{"CS2", "CS4", "CS1", "CS3", "CS5", "CS6", "CS7", "CS8"},
	} {
		fmt.Printf("\nkill order: %v\n", order)
		res, err := experiment.RunCascadingFailureExperiment(cfg, order)
		must(err)
		for _, r := range res.Rounds {
			printRound(r)
		}
		ids := make([]common.ServerID, 0)
		for id := range res.ConcentrationByNode {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		fmt.Print("  new replicas received (whole sequence):")
		for _, id := range ids {
			fmt.Printf(" %s=%d", id, res.ConcentrationByNode[id])
		}
		fmt.Println()
	}
}
