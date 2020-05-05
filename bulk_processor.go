package dorisloader

import (
	"context"
	"errors"
	"sync"
	"time"
)

type BulkProcessor struct {
	c                    *Client
	name                 string
	db                   string
	table                string
	bulkActions          int
	bulkSize             int
	flushInterval        time.Duration
	flusherStopC         chan struct{}
	retryItemStatusCodes map[int]struct{}
	numWorkers           int
	executionId          int64
	rows                 chan []byte
	workerWg             sync.WaitGroup
	workers              []*bulkWorker
	backoff              Backoff

	startedMu sync.Mutex
	started   bool

	stopReconnC chan struct{}
}

func NewBulkProcessor(
	client *Client,
	name string,
	db string,
	table string,
	numWorkers int,
	bulkActions int,
	bulkSize int,
	flushInterval time.Duration,
	backoff Backoff,
	retryItemStatusCodes map[int]struct{}) *BulkProcessor {
	return &BulkProcessor{
		c:                    client,
		name:                 name,
		db:                   db,
		table:                table,
		numWorkers:           numWorkers,
		bulkActions:          bulkActions,
		bulkSize:             bulkSize,
		flushInterval:        flushInterval,
		retryItemStatusCodes: retryItemStatusCodes,
		backoff:              backoff,
	}
}

func (p *BulkProcessor) Start(ctx context.Context) error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()

	if err := p.checkInterval(); err != nil {
		return err
	}

	if p.started {
		return nil
	}

	// We must have at least one worker.
	if p.numWorkers < 1 {
		p.numWorkers = 1
	}

	p.rows = make(chan []byte)
	p.executionId = 0
	p.stopReconnC = make(chan struct{})

	// Create and start up workers.
	p.workers = make([]*bulkWorker, p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		p.workerWg.Add(1)
		p.workers[i] = newBulkWorker(p, i)
		go p.workers[i].work(ctx)
	}

	// Start the ticker for flush (if enabled)
	if int64(p.flushInterval) > 0 {
		p.flusherStopC = make(chan struct{})
		go p.flusher(p.flushInterval)
	}

	p.started = true

	return nil
}

func (p *BulkProcessor) checkInterval() error {

	if p.bulkActions == 0 && p.bulkSize == 0 && p.flushInterval == 0 {
		return errors.New("bulk actions and bulk size and flush interval all is nil(0)")
	}

	return nil
}

// Stop is an alias for Close.
func (p *BulkProcessor) Stop() error {
	return p.Close()
}

func (p *BulkProcessor) Close() error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()

	// Already stopped? Do nothing.
	if !p.started {
		return nil
	}

	// Tell connection checkers to stop
	if p.stopReconnC != nil {
		close(p.stopReconnC)
		p.stopReconnC = nil
	}

	// Stop flusher (if enabled)
	if p.flusherStopC != nil {
		p.flusherStopC <- struct{}{}
		<-p.flusherStopC
		close(p.flusherStopC)
		p.flusherStopC = nil
	}

	// Stop all workers.
	close(p.rows)
	p.workerWg.Wait()

	p.started = false

	return nil
}

// Add adds a single request to commit by the BulkProcessorService.
//
// The caller is responsible for setting the index and type on the request.
func (p *BulkProcessor) Add(row []byte) {
	p.rows <- row
}

// Flush manually asks all workers to commit their outstanding requests.
// It returns only when all workers acknowledge completion.
func (p *BulkProcessor) Flush() error {

	for _, w := range p.workers {
		w.flushC <- struct{}{}
		<-w.flushAckC // wait for completion
	}

	return nil
}

// flusher is a single goroutine that periodically asks all workers to
// commit their outstanding bulk requests. It is only started if
// FlushInterval is greater than 0.
func (p *BulkProcessor) flusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Periodic flush
			p.Flush() // TODO swallow errors here?

		case <-p.flusherStopC:
			p.flusherStopC <- struct{}{}
			return
		}
	}
}

func (p *BulkProcessor) DB() string {
	return p.db
}

func (p *BulkProcessor) Table() string {
	return p.table
}
