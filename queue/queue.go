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

	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	waitCh := make(chan struct{})
	go func() {
		q.cond.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
	case <-timer.C:
	}

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
	q, ok := gqm.queues[name]
	if !ok {
		q = NewQueue()
		gqm.queues[name] = q
	}
	return q
}

func (gqm *GlobalQueueManager) Push(name string, item string) {
	gqm.getOrCreateQueue(name).Push(item)
}

func (gqm *GlobalQueueManager) Pop(name string) string {
	return gqm.getOrCreateQueue(name).Pop()
}

func (gqm *GlobalQueueManager) BPop(name string, timeoutMs int64) string {
	return gqm.getOrCreateQueue(name).BPop(timeoutMs)
}

func (gqm *GlobalQueueManager) Length(name string) int {
	return gqm.getOrCreateQueue(name).Length()
}

func (gqm *GlobalQueueManager) Clear(name string) {
	gqm.getOrCreateQueue(name).Clear()
}
