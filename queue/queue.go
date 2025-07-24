package queue

import (
	"context"
	"fmt"
	"sync" // For thread-safe queue operations

	"go.k6.io/k6/js/modules" // Import k6's JavaScript modules package
)

// Register the extension as a k6 module
func init() {
	modules.Register("k6/x/queue", new(Queue))
}

// Queue is the k6 extension struct.
// It will hold the state for our in-memory queue.
type Queue struct {
	// The actual queue data structure (a slice of interfaces to hold any type)
	data []interface{}
	mu   sync.Mutex // Mutex for thread-safe access to the queue data
}

// Enqueue adds an item to the queue.
// This function will be callable from k6 scripts as `queue.enqueue(item)`.
func (q *Queue) Enqueue(ctx context.Context, item interface{}) error {
	q.mu.Lock()         // Acquire lock before modifying the queue
	defer q.mu.Unlock() // Release lock when function exits

	q.data = append(q.data, item)
	fmt.Printf("Enqueued: %v (Queue size: %d)\n", item, len(q.data)) // Log for debugging
	return nil // Return nil for no error
}

// Dequeue removes and returns an item from the front of the queue.
// This function will be callable from k6 scripts as `queue.dequeue()`.
func (q *Queue) Dequeue(ctx context.Context) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.data) == 0 {
		return nil, fmt.Errorf("queue is empty") // Return error if queue is empty
	}

	item := q.data[0]       // Get the first item
	q.data = q.data[1:]     // Remove the first item by slicing
	fmt.Printf("Dequeued: %v (Queue size: %d)\n", item, len(q.data)) // Log for debugging
	return item, nil // Return the item and no error
}

// Size returns the current number of items in the queue.
// Callable as `queue.size()`.
func (q *Queue) Size(ctx context.Context) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.data)
}

// Clear removes all items from the queue.
// Callable as `queue.clear()`.
func (q *Queue) Clear(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.data = []interface{}{} // Reset the slice to an empty one
	fmt.Println("Queue cleared.")
}
