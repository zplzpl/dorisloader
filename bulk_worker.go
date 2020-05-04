package dorisloader

import (
	"context"
)

type bulkWorker struct {
	p           *BulkProcessor
	i           int
	bulkActions int
	bulkSize    int
	service     *BulkService
	flushC      chan struct{}
	flushAckC   chan struct{}
}

// newBulkWorker creates a new bulkWorker instance.
func newBulkWorker(p *BulkProcessor, i int) *bulkWorker {
	return &bulkWorker{
		p:           p,
		i:           i,
		bulkActions: p.bulkActions,
		bulkSize:    p.bulkSize,
		service:     NewBulkService(p.c).DB(p.db).Table(p.table),
		flushC:      make(chan struct{}),
		flushAckC:   make(chan struct{}),
	}
}

// work waits for bulk requests and manual flush calls on the respective
// channels and is invoked as a goroutine when the bulk processor is started.
func (w *bulkWorker) work(ctx context.Context) {

	defer func() {
		w.p.workerWg.Done()
		close(w.flushAckC)
		close(w.flushC)
	}()

	var stop bool
	for !stop {
		var err error
		select {
		case row, open := <-w.p.rows:
			if open {
				w.service.Add(row)
				if w.commitRequired() {
					err = w.commit(ctx)
				}
			} else {
				// Channel closed: Stop.
				stop = true
				if w.service.NumberOfRows() > 0 {
					err = w.commit(ctx)
				}
			}
		case <-w.flushC:
			// Commit outstanding requests
			if w.service.NumberOfRows() > 0 {
				err = w.commit(ctx)
			}
			w.flushAckC <- struct{}{}
		}
		if err != nil {
			if !stop {
				// TODO
			}
		}
	}
}

// commit commits the bulk requests in the given service,
// invoking callbacks as specified.
func (w *bulkWorker) commit(ctx context.Context) error {

	//var res *BulkResponse

	// commitFunc will commit bulk requests and, on failure, be retried
	// via exponential backoff
	commitFunc := func() error {
		var err error
		// Save requests because they will be reset in service.Do
		_, err = w.service.Do(ctx)
		if err != nil {
			return err
		}
		return nil
	}

	// notifyFunc will be called if retry fails
	notifyFunc := func(err error) {
		// TODO
	}

	// Commit bulk requests
	err := RetryNotify(commitFunc, w.p.backoff, notifyFunc)
	if err != nil {
		// TODO
	}

	return err
}

func (w *bulkWorker) commitRequired() bool {
	if w.bulkActions >= 0 && w.service.NumberOfRows() >= w.bulkActions {
		return true
	}
	if w.bulkSize >= 0 && w.service.EstimatedSizeInBytes() >= int64(w.bulkSize) {
		return true
	}
	return false
}
