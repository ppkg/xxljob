package xxljob

import (
	"context"
	"sync"

	"github.com/ppkg/glog"
	xxl "github.com/ppkg/xxl-job-executor-go"
)

type task struct{}

var (
	exec xxl.Executor
	jobs = make(map[string]func(cxt context.Context, param *xxl.RunReq) (msg string))
	lock sync.RWMutex
)

// appid：应用ID
// serverAddr：xxljob admin 接口地址，如 http://xx.xx.xx.xx:8080/xxl-job-admin
// executorPort：执行器服务端口
func Init(appid, serverAddr, executorPort string) *task {
	glog.Info("xxljob Init", appid, serverAddr, executorPort)
	exec = xxl.NewExecutor(
		xxl.ServerAddr(serverAddr),
		xxl.AccessToken(""),
		xxl.ExecutorPort(executorPort),
		xxl.RegistryKey(appid),
		xxl.SetLogger(&logger{}),
	)
	exec.Init()
	for k, v := range jobs {
		exec.RegTask(k, v)
	}
	return &task{}
}

// jobHandler：同一appid下不能重复
func AddTask(jobHandler string, jobFunc func(cxt context.Context, param *xxl.RunReq) (msg string)) {
	lock.Lock()
	defer lock.Unlock()
	if _, has := jobs[jobHandler]; has {
		glog.Error(jobHandler, "duplicate definition")
	} else {
		jobs[jobHandler] = jobFunc
	}
}

func (t *task) Run() {
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
