package xxljob

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ppkg/conngo"
	"github.com/ppkg/glog"
	xxl "github.com/ppkg/xxl-job-executor-go"
)

type task struct{}

var (
	exec        xxl.Executor
	jobs        = make(map[string]func(cxt context.Context, param *xxl.RunReq) (msg string))
	lock        sync.RWMutex
	redisConfig *conngo.Redis
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

// 添加标准的xxljob任务
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

// 添加根据最大更新时间取数据的任务
// 一般是需要根据redis中存储的更新时间取数据
// 取到数据后，再用最大的更新时间覆盖redis的原来的更新时间
func AddTaskMaxUpdateDate(jobHandler string, updateDateKey string, jobFunc func(cxt context.Context, param *xxl.RunReq) (msg string, updateDate time.Time)) {
	AddTask(jobHandler, func(cxt context.Context, param *xxl.RunReq) (msg string) {
		var redisUpdateDate string
		var updateDate time.Time
		msg, updateDate = jobFunc(cxt, param)
		redis := redisConfig.GetClient()
		for {
			k := fmt.Sprintf("xxljob:updateDate:%d", param.JobID)
			if redis.SetNX(k, param.JobID, time.Second*15).Val() {
				// 从redis获取当前的更新时间
				redisUpdateDate = redis.Get(updateDateKey).Val()

				redisT, _ := time.Parse("2006-01-02 15:04:05", redisUpdateDate) // redis存储的更新时间

				// 如果redis存储的时间小，则更新redis的时间
				if redisT.Before(updateDate) {
					redis.Set(updateDateKey, updateDate.Format("2006-01-02 15:04:05"), -1)
				}
				redis.Del(k)
				break
			}
			time.Sleep(time.Second * 1)
		}
		return
	})
}

func (t *task) Run() {
	if err := exec.Run(); err != nil {
		glog.Info("xxljob exec run error ", err)
	}
}

// 如果任务中有用到redis，需要设置redis信息
func (t *task) SetRedis(addr, password string, db int) *task {
	redisConfig = conngo.InitRedis(addr, password, db)
	return t
}

type logger struct{}

func (l *logger) Info(format string, a ...interface{}) {
	glog.Infof(format, a...)
}

func (l *logger) Error(format string, a ...interface{}) {
	glog.Errorf(format, a...)
}
