package xxljob

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/pb"
	"github.com/ppkg/glog"
	xxl "github.com/ppkg/xxl-job-executor-go"
)

type ShardInfo interface {
	Read(i []interface{}) error                      //读取分片数据
	Write(w io.Writer, is *pb.InstructionStat) error //写入需要分片的数据
	Run(cxt context.Context, param *xxl.RunReq) (msg string)
}

var mapper = make(map[string]gio.MapperId)
var lockMapper sync.RWMutex

// 注册分片任务
func RegisterShardTask(i ShardInfo) string {
	lockMapper.Lock()
	defer lockMapper.Unlock()
	gob.Register(i)
	name := strings.TrimLeft(reflect.TypeOf(i).String(), "*")
	mapper[name] = gio.RegisterMapper(i.Read)
	AddTask(name, i.Run)
	glog.Info("xxljob.RegisterShardTask", name)
	return name
}

func EncodeShardInfo(s ShardInfo) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(s); err != nil {
		log.Fatal("encode shard info:", err)
	}
	return network.Bytes()
}

// 生成分片信息
func Generate(i ShardInfo, f *flow.Flow) *flow.Dataset {
	if id, has := mapper[f.Name]; has {
		return f.Source(f.Name+".list", i.Write).RoundRobin(f.Name, runtime.NumCPU()).Map(f.Name+".Read", id)
	} else {
		glog.Error(f.Name, "not register")
		return nil
	}
}
