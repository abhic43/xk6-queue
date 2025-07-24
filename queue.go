package queue
import (
	"context"
	"sync"
	"time"

	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/queue", new(Queue))
}

// Queue represents the xk6 queue extension
type Queue struct{}

// QueueManager holds multiple named queues with thread-safe operations
type QueueManager struct {
	queues map[string]*SafeQueue
	mutex  sync.RWMutex
}

// SafeQueue is a thread-safe queue implementation
type SafeQueue struct {
	items []interface{}
	mutex sync.RWMutex
	cond  *sync.Cond
}

// Global queue manager instance
var globalQueueManager = &QueueManager{
	queues: make(map[string]*SafeQueue),
}

// NewModuleInstance creates a new instance of the Queue module
func (q *Queue) NewModuleInstance(vu modules.VU) modules.Instance {
	return &QueueInstance{
		vu: vu,
	}
}

// QueueInstance represents an instance of the queue module for a VU
type QueueInstance struct {
	vu modules.VU
}

// Exports returns the exports of the module
func (qi *QueueInstance) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"push":         qi.push,
			"pop":          qi.pop,
			"popWithTimeout": qi.popWithTimeout,
			"peek":         qi.peek,
			"size":         qi.size,
			"isEmpty":      qi.isEmpty,
			"clear":        qi.clear,
			"listQueues":   qi.listQueues,
		},
	}
}

// getOrCreateQueue gets an existing queue or creates a new one
func (qm *QueueManager) getOrCreateQueue(name string) *SafeQueue {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	if queue, exists := qm.queues[name]; exists {
		return queue
	}

	queue := &SafeQueue{
		items: make([]interface{}, 0),
	}
	queue.cond = sync.NewCond(&queue.mutex)
	qm.queues[name] = queue
	return queue
}

// Push adds an item to the specified queue
func (qi *QueueInstance) push(queueName string, item interface{}) {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	queue.items = append(queue.items, item)
	queue.cond.Signal() // Wake up any waiting pop operations
}

// Pop removes and returns the first item from the queue
// Returns null if queue is empty
func (qi *QueueInstance) pop(queueName string) interface{} {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if len(queue.items) == 0 {
		return nil
	}

	item := queue.items[0]
	queue.items = queue.items[1:]
	return item
}

// PopWithTimeout removes and returns the first item from the queue
// Waits up to the specified timeout (in milliseconds) for an item to become available
func (qi *QueueInstance) popWithTimeout(queueName string, timeoutMs int) interface{} {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	// If queue has items, return immediately
	if len(queue.items) > 0 {
		item := queue.items[0]
		queue.items = queue.items[1:]
		return item
	}

	// Wait for an item with timeout
	timeout := time.Duration(timeoutMs) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan bool, 1)
	go func() {
		queue.cond.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Item available, check again
		if len(queue.items) > 0 {
			item := queue.items[0]
			queue.items = queue.items[1:]
			return item
		}
		return nil
	case <-timer.C:
		// Timeout reached
		return nil
	}
}

// Peek returns the first item without removing it
func (qi *QueueInstance) peek(queueName string) interface{} {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()

	if len(queue.items) == 0 {
		return nil
	}

	return queue.items[0]
}

// Size returns the number of items in the queue
func (qi *QueueInstance) size(queueName string) int {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()

	return len(queue.items)
}

// IsEmpty returns true if the queue is empty
func (qi *QueueInstance) isEmpty(queueName string) bool {
	return qi.size(queueName) == 0
}

// Clear removes all items from the queue
func (qi *QueueInstance) clear(queueName string) {
	queue := globalQueueManager.getOrCreateQueue(queueName)
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	queue.items = queue.items[:0] // Clear slice but keep capacity
}

// ListQueues returns the names of all created queues
func (qi *QueueInstance) listQueues() []string {
	globalQueueManager.mutex.RLock()
	defer globalQueueManager.mutex.RUnlock()

	names := make([]string, 0, len(globalQueueManager.queues))
	for name := range globalQueueManager.queues {
		names = append(names, name)
	}
	return names
}