package xxljob

import (
	"context"
	"sync"

	"github.com/ppkg/glog"
	xxl "github.com/ppkg/xxl-job-executor-go"
)

type Task struct{}

var (
	exec xxl.Executor
	jobs map[string]func(cxt context.Context, param *xxl.RunReq) (msg string)
	lock sync.RWMutex
)

func InitTask(key, addr, port string) *Task {
	glog.Info("xxljob Init", key, addr, port)
	exec = xxl.NewExecutor(
		xxl.ServerAddr(addr),
		xxl.AccessToken(""),
		xxl.ExecutorPort(port),
		xxl.RegistryKey(key),
		xxl.SetLogger(&logger{}),
	)
	exec.Init()
	for k, v := range jobs {
		exec.RegTask(k, v)
	}
	return &Task{}
}

func AddTask(jobHandler string, jobFunc func(cxt context.Context, param *xxl.RunReq) (msg string)) {
	lock.Lock()
	defer lock.Unlock()
	if _, has := jobs[jobHandler]; has {
		glog.Error(jobHandler, "duplicate definition")
	} else {
		jobs[jobHandler] = jobFunc
	}
}

func (t *Task) TaskRun() {
	if err := exec.Run(); err != nil {
		glog.Info("xxljob exec run error ", err)
	}
}

type logger struct{}

func (l *logger) Info(format string, a ...interface{}) {
	glog.Infof(format, a...)
}

func (l *logger) Error(format string, a ...interface{}) {
	glog.Errorf(format, a...)
}
