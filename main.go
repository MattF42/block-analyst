// block-analyst queries a Hoosat HTND node via its native gRPC RPC and
// produces a tabulated report of coinbase-reward recipients for the last
// N minutes of blocks.
//
// Usage:
//
//	block-analyst [flags]
//
//	  --rpcserver  string  HTND gRPC address (default "localhost:16110")
//	  --minutes    int     How many minutes of history to scan (default 60)
//	  --atoms      bool    Show reward column in atoms instead of HTN
//	  --chain-only bool    Count only chain (blue) blocks; skip merge-set
//	                       reds/blues (default true)
//	  --verbose    bool    Print each block hash as it is scanned
//
// Example:
//
//	block-analyst --rpcserver 192.168.1.10:42420 --minutes 30
//
// The tool connects directly to HTND's gRPC port (the same port used by
// htnctl), walks the selected-parent chain backwards from the DAG tip for
// the requested time window, collects coinbase payout addresses from the
// first transaction in each block, and prints a table sorted by blocks
// found (most prolific miner first).
//
// HTND is a GhostDAG network that runs at ~5 BPS.  The selected-parent
// chain covers only ~1–2 BPS; the remaining blocks live in each chain
// block's merge set (MergeSetBluesHashes + MergeSetRedsHashes).  When
// --chain-only=false the tool fetches those merge-set blocks too, giving
// a complete picture of all coinbase rewards.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/infrastructure/network/rpcclient/grpcclient"
)

// normalizeAddr strips an "http://" or "https://" scheme so that users who
// copy-paste a URL still get a working host:port address.
func normalizeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	addr = strings.TrimPrefix(addr, "https://")
	addr = strings.TrimPrefix(addr, "http://")
	if idx := strings.IndexByte(addr, '/'); idx != -1 {
		addr = addr[:idx]
	}
	return addr
}

// getSelectedTipHash fetches the virtual selected tip hash from the node.
func getSelectedTipHash(c *grpcclient.GRPCClient) (string, error) {
	resp, err := c.PostAppMessage(appmessage.NewGetSelectedTipHashRequestMessage())
	if err != nil {
		return "", fmt.Errorf("GetSelectedTipHash: %w", err)
	}
	r, ok := resp.(*appmessage.GetSelectedTipHashResponseMessage)
	if !ok {
		return "", fmt.Errorf("GetSelectedTipHash: unexpected response type %T", resp)
	}
	if r.Error != nil {
		return "", fmt.Errorf("GetSelectedTipHash RPC error: %s", r.Error.Message)
	}
	return r.SelectedTipHash, nil
}

// getBlock fetches a block by hash.  Pass includeTransactions=true to also
// receive the full transaction list (needed for coinbase inspection).
func getBlock(c *grpcclient.GRPCClient, hash string, includeTransactions bool) (*appmessage.RPCBlock, error) {
	short := hash
	if len(short) > 16 {
		short = short[:16]
	}
	resp, err := c.PostAppMessage(appmessage.NewGetBlockRequestMessage(hash, includeTransactions))
	if err != nil {
		return nil, fmt.Errorf("GetBlock %s...: %w", short, err)
	}
	r, ok := resp.(*appmessage.GetBlockResponseMessage)
	if !ok {
		return nil, fmt.Errorf("GetBlock %s...: unexpected response type %T", short, resp)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("GetBlock %s... RPC error: %s", short, r.Error.Message)
	}
	return r.Block, nil
}

// ─── Analysis ────────────────────────────────────────────────────────────────

type minerStats struct {
	Address      string
	BlocksFound  int
	TotalAtoms   uint64
	FirstBlockAt time.Time
	LastBlockAt  time.Time
}

type report struct {
	Miners        []*minerStats
	ChainBlocks   int // blocks on the selected chain
	NonChainBlocks int // merge-set blocks fetched (only when !chainOnly)
	OldestBlock   time.Time
	NewestBlock   time.Time
	WindowMinutes int
	ChainOnly     bool
}

func analyze(rpcAddr string, windowMins int, chainOnly bool, verbose bool) (*report, error) {
	c, err := grpcclient.Connect(rpcAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", rpcAddr, err)
	}
	defer func() { _ = c.Disconnect() }()

	tip, err := getSelectedTipHash(c)
	if err != nil {
		return nil, err
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "DAG tip: %s\n", tip)
	}

	cutoff := time.Now().Add(-time.Duration(windowMins) * time.Minute)
	addrMap := make(map[string]*minerStats)
	r := &report{WindowMinutes: windowMins, ChainOnly: chainOnly}
	hash := tip

	// processBlock attributes a block's coinbase to its payout address(es) and
	// updates the report time window.  It returns false if the block falls
	// outside the requested time window.
	processBlock := func(block *appmessage.RPCBlock) bool {
		blockTimeMs := block.Header.Timestamp
		blockTime := time.UnixMilli(blockTimeMs)

		if blockTimeMs > 0 && blockTime.Before(cutoff) {
			return false
		}

		if r.NewestBlock.IsZero() || blockTime.After(r.NewestBlock) {
			r.NewestBlock = blockTime
		}
		if r.OldestBlock.IsZero() || blockTime.Before(r.OldestBlock) {
			r.OldestBlock = blockTime
		}

		for addr, atoms := range coinbasePayouts(block) {
			ms, ok := addrMap[addr]
			if !ok {
				ms = &minerStats{Address: addr}
				addrMap[addr] = ms
			}
			ms.BlocksFound++
			ms.TotalAtoms += atoms
			if ms.LastBlockAt.IsZero() || blockTime.After(ms.LastBlockAt) {
				ms.LastBlockAt = blockTime
			}
			if ms.FirstBlockAt.IsZero() || blockTime.Before(ms.FirstBlockAt) {
				ms.FirstBlockAt = blockTime
			}
		}
		return true
	}

	for {
		block, err := getBlock(c, hash, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v — stopping walk\n", err)
			break
		}
		if block == nil {
			break
		}

		blockTime := time.UnixMilli(block.Header.Timestamp)

		if verbose {
			short := hash
			if len(short) > 16 {
				short = short[:16]
			}
			fmt.Fprintf(os.Stderr, "  [chain %d] %s...  %s\n",
				r.ChainBlocks+1,
				short,
				blockTime.UTC().Format("2006-01-02 15:04:05"),
			)
		}

		// Stop once we are past the time window.
		if block.Header.Timestamp > 0 && blockTime.Before(cutoff) {
			break
		}

		r.ChainBlocks++
		processBlock(block)

		// When not chain-only, also process each block in this chain block's
		// merge set.  In GhostDAG the merge sets of successive chain blocks
		// partition the DAG, so there is no overlap and no deduplication is
		// needed.
		if !chainOnly && block.VerboseData != nil {
			mergeHashes := append(
				block.VerboseData.MergeSetBluesHashes,
				block.VerboseData.MergeSetRedsHashes...,
			)
			for _, msHash := range mergeHashes {
				msBlock, err := getBlock(c, msHash, true)
				if err != nil {
					fmt.Fprintf(os.Stderr, "warning: merge-set block: %v\n", err)
					continue
				}
				if msBlock == nil {
					continue
				}
				r.NonChainBlocks++
				if verbose {
					short := msHash
					if len(short) > 16 {
						short = short[:16]
					}
					msTime := time.UnixMilli(msBlock.Header.Timestamp)
					fmt.Fprintf(os.Stderr, "  [merge  %d] %s...  %s\n",
						r.NonChainBlocks,
						short,
						msTime.UTC().Format("2006-01-02 15:04:05"),
					)
				}
				processBlock(msBlock)
			}
		}

		// Navigate to the selected parent.
		next := ""
		if block.VerboseData != nil {
			next = block.VerboseData.SelectedParentHash
		}
		// Fall back to level-0 first parent if verboseData is absent.
		if next == "" && len(block.Header.Parents) > 0 && len(block.Header.Parents[0].ParentHashes) > 0 {
			next = block.Header.Parents[0].ParentHashes[0]
		}
		if next == "" || next == hash {
			break // genesis or loop guard
		}
		hash = next
	}

	// Sort by blocks found descending; use total atoms as tiebreaker.
	for _, ms := range addrMap {
		r.Miners = append(r.Miners, ms)
	}
	sort.Slice(r.Miners, func(i, j int) bool {
		if r.Miners[i].BlocksFound != r.Miners[j].BlocksFound {
			return r.Miners[i].BlocksFound > r.Miners[j].BlocksFound
		}
		return r.Miners[i].TotalAtoms > r.Miners[j].TotalAtoms
	})

	return r, nil
}

// coinbasePayouts extracts address→atoms from the first transaction's outputs.
func coinbasePayouts(block *appmessage.RPCBlock) map[string]uint64 {
	out := make(map[string]uint64)
	if len(block.Transactions) == 0 {
		return out
	}
	for _, o := range block.Transactions[0].Outputs {
		addr := "(unknown)"
		if o.VerboseData != nil && o.VerboseData.ScriptPublicKeyAddress != "" {
			addr = strings.ToLower(strings.TrimSpace(o.VerboseData.ScriptPublicKeyAddress))
		}
		out[addr] += o.Amount
	}
	return out
}

// ─── Formatting ──────────────────────────────────────────────────────────────

const atomsPerHTN = 100_000_000

func fmtHTN(atoms uint64) string {
	f := float64(atoms) / float64(atomsPerHTN)
	f = math.Round(f*10000) / 10000
	return fmt.Sprintf("%.4f", f)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	half := (maxLen - 3) / 2
	return s[:half] + "..." + s[len(s)-half:]
}

func printReport(r *report, showAtoms bool) {
	totalBlocks := r.ChainBlocks + r.NonChainBlocks
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════")
	fmt.Printf("  Hoosat Block Miner Report  —  last %d minutes\n", r.WindowMinutes)
	if r.ChainOnly {
		fmt.Printf("  Scanned %d chain blocks  (non-chain merge-set blocks not fetched)\n", r.ChainBlocks)
	} else {
		fmt.Printf("  Scanned %d blocks  (%d chain + %d merge-set non-chain)\n",
			totalBlocks, r.ChainBlocks, r.NonChainBlocks)
	}
	if !r.OldestBlock.IsZero() {
		fmt.Printf("  Window: %s → %s\n",
			r.OldestBlock.UTC().Format("2006-01-02 15:04:05"),
			r.NewestBlock.UTC().Format("2006-01-02 15:04:05 UTC"))
	}
	fmt.Printf("  %d unique payout address(es)\n", len(r.Miners))
	fmt.Println("═══════════════════════════════════════════════════════════════════════")
	fmt.Println()

	if len(r.Miners) == 0 {
		fmt.Println("  No data.")
		return
	}

	rewardHdr := "Reward (HTN)"
	if showAtoms {
		rewardHdr = "Reward (atoms)"
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Rank\tAddress\tBlocks\tShare%%\t%s\tFirst Block\tLast Block\n", rewardHdr)
	fmt.Fprintf(w, "────\t───────\t──────\t───────\t%s\t───────────\t──────────\n",
		strings.Repeat("─", len(rewardHdr)))

	totalAddrBlocks := 0
	for _, ms := range r.Miners {
		totalAddrBlocks += ms.BlocksFound
	}

	for i, ms := range r.Miners {
		sharePct := 0.0
		if totalAddrBlocks > 0 {
			sharePct = float64(ms.BlocksFound) / float64(totalAddrBlocks) * 100
		}
		reward := fmtHTN(ms.TotalAtoms)
		if showAtoms {
			reward = fmt.Sprintf("%d", ms.TotalAtoms)
		}
		firstStr, lastStr := "—", "—"
		if !ms.FirstBlockAt.IsZero() {
			firstStr = ms.FirstBlockAt.UTC().Format("15:04:05")
		}
		if !ms.LastBlockAt.IsZero() {
			lastStr = ms.LastBlockAt.UTC().Format("15:04:05")
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%.1f%%\t%s\t%s\t%s\n",
			i+1, truncate(ms.Address, 52),
			ms.BlocksFound, sharePct,
			reward, firstStr, lastStr,
		)
	}
	_ = w.Flush()
	fmt.Println()

	var totalAtoms uint64
	for _, ms := range r.Miners {
		totalAtoms += ms.TotalAtoms
	}
	if showAtoms {
		fmt.Printf("  Total coinbase issued: %d atoms\n", totalAtoms)
	} else {
		fmt.Printf("  Total coinbase issued: %s HTN\n", fmtHTN(totalAtoms))
	}
	if !r.OldestBlock.IsZero() && !r.NewestBlock.IsZero() && totalBlocks > 0 {
		elapsed := r.NewestBlock.Sub(r.OldestBlock).Seconds()
		if elapsed > 0 {
			bps := float64(totalBlocks) / elapsed
			fmt.Printf("  Block rate: %.3f blocks/sec  (~%.1f blocks/min)\n", bps, bps*60)
		}
	}
	fmt.Println()
}

// ─── Entry point ─────────────────────────────────────────────────────────────

func main() {
	rpcServer := flag.String("rpcserver", "localhost:16110",
		"HTND gRPC address (host:port).  Also accepts http://host:port — the scheme is stripped.")
	minutes   := flag.Int("minutes", 60, "How many minutes of block history to analyse")
	showAtoms := flag.Bool("atoms", false, "Show reward column in atoms instead of HTN")
	chainOnly := flag.Bool("chain-only", true, "Only count chain (blue) blocks")
	verbose   := flag.Bool("verbose", false, "Print each block hash as it is scanned")
	flag.Parse()

	if *minutes <= 0 {
		fmt.Fprintln(os.Stderr, "error: --minutes must be > 0")
		os.Exit(1)
	}

	addr := normalizeAddr(*rpcServer)
	fmt.Fprintf(os.Stderr, "Connecting to HTND at %s ...\n", addr)
	fmt.Fprintf(os.Stderr, "Scanning last %d minutes of blocks (chain-only=%v)\n\n", *minutes, *chainOnly)

	r, err := analyze(addr, *minutes, *chainOnly, *verbose)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	printReport(r, *showAtoms)
}
