package main

import (
	"github.com/grafana/k6/js/modules"
	"github.com/grafana/k6/lib/types"
	"github.com/grafana/k6/options"
	"github.com/your-username/xk6-queue/queue"
)

var globalQueueManager = queue.NewGlobalQueueManager()

func init() {
	modules.Register("k6/x/queue", new(RootModule))
}

type RootModule struct{}

func (*RootModule) NewModuleInstance(vu types.VU, o *options.Options) (modules.Module, error) {
	return &Module{vu: vu, manager: globalQueueManager}, nil
}

type Module struct {
	vu      types.VU
	manager *queue.GlobalQueueManager
}

func (m *Module) Exports() modules.Exports {
	return modules.Exports{
		Default: m,
	}
}

func (m *Module) Push(name string, item string) {
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
