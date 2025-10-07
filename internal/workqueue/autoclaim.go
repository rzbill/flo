package workqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	"github.com/rzbill/flo/pkg/log"
)

// AutoClaimScanner periodically scans for expired leases and reassigns them to active consumers.
type AutoClaimScanner struct {
	db        *pebblestore.DB
	namespace string
	name      string
	interval  time.Duration
	batchSize int
	logger    log.Logger

	leaseMgr     *LeaseManager
	consumerReg  *ConsumerRegistry
	defaultLease time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	groups map[string]bool // Track groups to scan
}

// AutoClaimConfig configures the auto-claim scanner.
type AutoClaimConfig struct {
	Interval     time.Duration // How often to scan (default: 2s)
	BatchSize    int           // Max leases to claim per scan (default: 100)
	DefaultLease time.Duration // Lease duration for claimed messages (default: 30s)
}

// NewAutoClaimScanner creates a new auto-claim scanner.
func NewAutoClaimScanner(db *pebblestore.DB, namespace, name string, cfg AutoClaimConfig, logger log.Logger) *AutoClaimScanner {
	if cfg.Interval == 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.DefaultLease == 0 {
		cfg.DefaultLease = 30 * time.Second
	}
	if logger == nil {
		// Create a minimal logger if none provided
		logger = log.NewLogger(log.WithLevel(log.InfoLevel))
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &AutoClaimScanner{
		db:           db,
		namespace:    namespace,
		name:         name,
		interval:     cfg.Interval,
		batchSize:    cfg.BatchSize,
		defaultLease: cfg.DefaultLease,
		logger:       logger,
		leaseMgr:     NewLeaseManager(db, namespace, name),
		consumerReg:  NewConsumerRegistry(db, namespace, name, 15*time.Second),
		ctx:          ctx,
		cancel:       cancel,
		groups:       make(map[string]bool),
	}
}

// Start begins the auto-claim scanner.
func (acs *AutoClaimScanner) Start() {
	acs.wg.Add(1)
	go acs.run()
}

// Stop gracefully stops the scanner.
func (acs *AutoClaimScanner) Stop() {
	acs.cancel()
	acs.wg.Wait()
}

// RegisterGroup adds a group to scan for expired leases.
func (acs *AutoClaimScanner) RegisterGroup(group string) {
	acs.mu.Lock()
	defer acs.mu.Unlock()
	acs.groups[group] = true
}

// UnregisterGroup removes a group from scanning.
func (acs *AutoClaimScanner) UnregisterGroup(group string) {
	acs.mu.Lock()
	defer acs.mu.Unlock()
	delete(acs.groups, group)
}

// run is the main scanner loop.
func (acs *AutoClaimScanner) run() {
	defer acs.wg.Done()

	ticker := time.NewTicker(acs.interval)
	defer ticker.Stop()

	acs.logger.Info("AutoClaimScanner started",
		log.Field{Key: "namespace", Value: acs.namespace},
		log.Field{Key: "queue", Value: acs.name},
		log.Field{Key: "interval", Value: acs.interval.String()},
	)

	for {
		select {
		case <-acs.ctx.Done():
			acs.logger.Info("AutoClaimScanner stopped")
			return
		case <-ticker.C:
			acs.scanAndClaim()
		}
	}
}

// scanAndClaim scans for expired leases and claims them.
func (acs *AutoClaimScanner) scanAndClaim() {
	acs.mu.RLock()
	groups := make([]string, 0, len(acs.groups))
	for group := range acs.groups {
		groups = append(groups, group)
	}
	acs.mu.RUnlock()

	for _, group := range groups {
		if err := acs.scanGroup(group); err != nil {
			acs.logger.Error("AutoClaimScanner: scan group failed",
				log.Field{Key: "group", Value: group},
				log.Field{Key: "error", Value: err.Error()},
			)
		}
	}
}

// scanGroup scans a single group for expired leases.
func (acs *AutoClaimScanner) scanGroup(group string) error {
	ctx := acs.ctx

	// 1. List expired leases
	expiredLeases, err := acs.leaseMgr.ListExpiredLeases(ctx, group, acs.batchSize)
	if err != nil {
		return fmt.Errorf("list expired leases: %w", err)
	}

	if len(expiredLeases) == 0 {
		return nil
	}

	acs.logger.Debug("AutoClaimScanner: found expired leases",
		log.Field{Key: "group", Value: group},
		log.Field{Key: "count", Value: len(expiredLeases)},
	)

	// 2. Check for active consumers
	activeConsumer, err := acs.consumerReg.SelectConsumer(ctx, group)
	if err != nil {
		// No active consumers, messages will remain expired until consumers register
		acs.logger.Debug("AutoClaimScanner: no active consumers",
			log.Field{Key: "group", Value: group},
		)
		return nil
	}

	// 3. Claim each expired lease
	claimed := 0
	for _, lease := range expiredLeases {
		// Check if original consumer is still active
		if acs.consumerReg.IsActive(ctx, group, lease.ConsumerID) {
			// Consumer is still active, might be slow - don't claim yet
			// TODO: Could check delivery count and claim if too many retries
			continue
		}

		// Claim the lease
		if err := acs.leaseMgr.ClaimLease(ctx, group, lease.MessageID, activeConsumer, acs.defaultLease.Milliseconds()); err != nil {
			acs.logger.Error("AutoClaimScanner: claim failed",
				log.Field{Key: "group", Value: group},
				log.Field{Key: "message_id", Value: fmt.Sprintf("%x", lease.MessageID)},
				log.Field{Key: "old_consumer", Value: lease.ConsumerID},
				log.Field{Key: "new_consumer", Value: activeConsumer},
				log.Field{Key: "error", Value: err.Error()},
			)
			continue
		}

		claimed++
		acs.logger.Debug("AutoClaimScanner: claimed message",
			log.Field{Key: "group", Value: group},
			log.Field{Key: "message_id", Value: fmt.Sprintf("%x", lease.MessageID)},
			log.Field{Key: "old_consumer", Value: lease.ConsumerID},
			log.Field{Key: "new_consumer", Value: activeConsumer},
			log.Field{Key: "delivery_count", Value: lease.DeliveryCount},
		)

		// Select a new consumer for each message (round-robin effect)
		activeConsumer, _ = acs.consumerReg.SelectConsumer(ctx, group)
	}

	if claimed > 0 {
		acs.logger.Info("AutoClaimScanner: claimed messages",
			log.Field{Key: "group", Value: group},
			log.Field{Key: "claimed", Value: claimed},
			log.Field{Key: "total_expired", Value: len(expiredLeases)},
		)
	}

	return nil
}

// ManualClaim allows explicit claim of a specific message.
// Useful for admin operations or consumer-initiated claims.
func (acs *AutoClaimScanner) ManualClaim(ctx context.Context, group string, msgID []byte, consumerID string) error {
	return acs.leaseMgr.ClaimLease(ctx, group, msgID, consumerID, acs.defaultLease.Milliseconds())
}

// GetStats returns statistics about the auto-claim scanner.
func (acs *AutoClaimScanner) GetStats(ctx context.Context, group string) (map[string]interface{}, error) {
	// Get expired lease count
	expiredLeases, err := acs.leaseMgr.ListExpiredLeases(ctx, group, 1000)
	if err != nil {
		return nil, fmt.Errorf("list expired leases: %w", err)
	}

	// Get active consumer count
	activeConsumers, err := acs.consumerReg.List(ctx, group, 1000)
	if err != nil {
		return nil, fmt.Errorf("list consumers: %w", err)
	}

	now := time.Now().UnixMilli()
	active := 0
	for _, c := range activeConsumers {
		if c.ExpiresAtMs > now {
			active++
		}
	}

	return map[string]interface{}{
		"expired_leases":   len(expiredLeases),
		"active_consumers": active,
		"total_consumers":  len(activeConsumers),
		"interval_ms":      acs.interval.Milliseconds(),
		"batch_size":       acs.batchSize,
	}, nil
}

// ConsumerSweeper periodically removes expired consumers.
type ConsumerSweeper struct {
	consumerReg *ConsumerRegistry
	interval    time.Duration
	batchSize   int
	logger      log.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	groups map[string]bool
}

// NewConsumerSweeper creates a new consumer sweeper.
func NewConsumerSweeper(db *pebblestore.DB, namespace, name string, interval time.Duration, logger log.Logger) *ConsumerSweeper {
	if interval == 0 {
		interval = 10 * time.Second
	}
	if logger == nil {
		// Create a minimal logger if none provided
		logger = log.NewLogger(log.WithLevel(log.InfoLevel))
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerSweeper{
		consumerReg: NewConsumerRegistry(db, namespace, name, 15*time.Second),
		interval:    interval,
		batchSize:   100,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		groups:      make(map[string]bool),
	}
}

// Start begins the consumer sweeper.
func (cs *ConsumerSweeper) Start() {
	cs.wg.Add(1)
	go cs.run()
}

// Stop gracefully stops the sweeper.
func (cs *ConsumerSweeper) Stop() {
	cs.cancel()
	cs.wg.Wait()
}

// RegisterGroup adds a group to sweep.
func (cs *ConsumerSweeper) RegisterGroup(group string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.groups[group] = true
}

// UnregisterGroup removes a group from sweeping.
func (cs *ConsumerSweeper) UnregisterGroup(group string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.groups, group)
}

// run is the main sweeper loop.
func (cs *ConsumerSweeper) run() {
	defer cs.wg.Done()

	ticker := time.NewTicker(cs.interval)
	defer ticker.Stop()

	cs.logger.Info("ConsumerSweeper started",
		log.Field{Key: "interval", Value: cs.interval.String()},
	)

	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Info("ConsumerSweeper stopped")
			return
		case <-ticker.C:
			cs.sweep()
		}
	}
}

// sweep removes expired consumers.
func (cs *ConsumerSweeper) sweep() {
	cs.mu.RLock()
	groups := make([]string, 0, len(cs.groups))
	for group := range cs.groups {
		groups = append(groups, group)
	}
	cs.mu.RUnlock()

	for _, group := range groups {
		cleaned, err := cs.consumerReg.CleanupExpired(cs.ctx, group, cs.batchSize)
		if err != nil {
			cs.logger.Error("ConsumerSweeper: cleanup failed",
				log.Field{Key: "group", Value: group},
				log.Field{Key: "error", Value: err.Error()},
			)
			continue
		}

		if cleaned > 0 {
			cs.logger.Info("ConsumerSweeper: cleaned expired consumers",
				log.Field{Key: "group", Value: group},
				log.Field{Key: "count", Value: cleaned},
			)
		}
	}
}
