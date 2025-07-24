package queue

import (
	"container/list"
	"sync"
	"time"
)

type Item struct {
	Value string
}

type Queue struct {
	mu    sync.Mutex
	items *list.List
	cond  *sync.Cond
}

func NewQueue() *Queue {
	q := &Queue{
		items: list.New(),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Push(item string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items.PushBack(&Item{Value: item})
	q.cond.Signal()
}

func (q *Queue) Pop() string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.items.Len() == 0 {
		return ""
	}
	element := q.items.Front()
	q.items.Remove(element)
	return element.Value.(*Item).Value
}

func (q *Queue) BPop(timeoutMs int64) string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.items.Len() > 0 {
		element := q.items.Front()
		q.items.Remove(element)
		return element.Value.(*Item).Value
	}

	if timeoutMs <= 0 {
		return ""
	}

	done := make(chan struct{})
	go func() {
		q.cond.L.Lock()
		defer q.cond.L.Unlock()

		select {
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			close(done)
			return
		default:
			q.cond.Wait()
			close(done)
		}
	}()

	<-done

	if q.items.Len() > 0 {
		element := q.items.Front()
		q.items.Remove(element)
		return element.Value.(*Item).Value
	}
	return ""
}

func (q *Queue) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items.Len()
}

func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = list.New()
}

type GlobalQueueManager struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func NewGlobalQueueManager() *GlobalQueueManager {
	return &GlobalQueueManager{
		queues: make(map[string]*Queue),
	}
}

func (gqm *GlobalQueueManager) getOrCreateQueue(name string) *Queue {
	gqm.mu.Lock()
	defer gqm.mu.Unlock()

	q, exists := gqm.queues[name]
	if !exists {
		q = NewQueue()
		gqm.queues[name] = q
	}
	return q
}

func (gqm *GlobalQueueManager) Push(name string, item string) {
	q := gqm.getOrCreateQueue(name)
	q.Push(item)
}

func (gqm *GlobalQueueManager) Pop(name string) string {
	q := gqm.getOrCreateQueue(name)
	return q.Pop()
}

func (gqm *GlobalQueueManager) BPop(name string, timeoutMs int64) string {
	q := gqm.getOrCreateQueue(name)
	return q.BPop(timeoutMs)
}

func (gqm *GlobalQueueManager) Length(name string) int {
	q := gqm.getOrCreateQueue(name)
	return q.Length()
}

func (gqm *GlobalQueueManager) Clear(name string) {
	q := gqm.getOrCreateQueue(name)
	q.Clear()
}
