package main

import (
	"github.com/abhic43/xk6-queue/queue"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib/types"
	"go.k6.io/k6/lib"
)

var globalQueueManager = queue.NewGlobalQueueManager()

func init() {
	modules.Register("k6/x/queue", new(RootModule))
}

type RootModule struct{}

func (*RootModule) NewModuleInstance(vu lib.VU, _ *lib.Options) (modules.Module, error) {
	return &Module{vu: vu, manager: globalQueueManager}, nil
}

type Module struct {
	vu      lib.VU
	manager *queue.GlobalQueueManager
}

func (m *Module) Exports() modules.Exports {
	return modules.Exports{
		Default: m,
	}
}

func (m *Module) Push(name, item string) {
	m.manager.Push(name, item)
}

func (m *Module) Pop(name string) string {
	return m.manager.Pop(name)
}

func (m *Module) BPop(name string, timeoutMs int64) string {
	return m.manager.BPop(name, timeoutMs)
}

func (m *Module) Length(name string) int {
	return m.manager.Length(name)
}

func (m *Module) Clear(name string) {
	m.manager.Clear(name)
}
