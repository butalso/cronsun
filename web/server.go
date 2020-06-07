package main

import (
	"flag"
	"github.com/butalso/cronsun/common/conf"
	"github.com/butalso/cronsun/common/db"
	"github.com/butalso/cronsun/common/etcd"
	"github.com/butalso/cronsun/web/dal/mgo"
	"github.com/butalso/cronsun/web/notice"
	"github.com/butalso/cronsun/web/service"
	"net"
	"time"

	"github.com/butalso/cronsun/common/event"
	"github.com/butalso/cronsun/common/log"
	"github.com/cockroachdb/cmux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	level    = flag.Int("l", 0, "log level, -1:debug, 0:info, 1:warn, 2:error")
	confFile = flag.String("conf", "conf/files/base.json", "config file path")
	network  = flag.String("network", "", "network protocol of listen address: ipv4/ipv6, or empty use both")
)

func main() {
	flag.Parse()

	// 初始化日志
	lcf := zap.NewDevelopmentConfig()
	lcf.Level.SetLevel(zapcore.Level(*level))
	lcf.Development = false
	logger, err := lcf.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}
	log.SetLogger(logger.Sugar())

	// 加载配置文件
	if err = conf.Init(*confFile, true); err != nil {
		panic(err)
	}

	// init etcd client
	if err = etcd.InitEtcdClient(conf.Config); err != nil {
		panic(err)
	}

	// init mongoDB
	if err = db.InitMgo(conf.Config.Mgo); err != nil {
		panic(err)
	}
	mgo.EnsureJobLogIndex()
	
	// 监视etcd，发送通知
	if conf.Config.Mail.Enable {
		var noticer notice.Noticer

		if len(conf.Config.Mail.HttpAPI) > 0 {
			noticer = &notice.HttpAPI{}
		} else {
			mailer, err := notice.NewMail(30 * time.Second)
			if err != nil {
				log.Errorf(err.Error())
				return
			}
			noticer = mailer
		}
		go notice.StartNoticer(noticer)
	}

	// 日志定时清理
	period := int64(conf.Config.Web.LogCleaner.EveryMinute)
	var stopCleaner func(interface{})
	if period > 0 {
		closeChan := asyncLogCleaner(time.Duration(period)*time.Minute, time.Duration(conf.Config.Web.LogCleaner.ExpirationDays)*time.Hour*24)
		stopCleaner = func(i interface{}) {
			close(closeChan)
		}
	}
	
	// 启动http服务器
	l, err := net.Listen(checkNetworkProtocol(*network), conf.Config.Web.BindAddr)
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	// Create a cmux.
	m := cmux.New(l)
	httpL := m.Match(cmux.HTTP1Fast())
	httpServer, err := service.InitServer()
	if err != nil {
		log.Errorf(err.Error())
		return
	}
	go func() {
		err := httpServer.Serve(httpL)
		if err != nil {
			panic(err.Error())
		}
	}()
	go m.Serve()
	log.Infof("cronsun web server started on %s, Ctrl+C or send kill sign to exit", conf.Config.Web.BindAddr)
	
	// 注册退出事件
	event.On(event.EXIT, conf.Exit, stopCleaner)
	// 监听退出信号
	event.Wait()
	event.Emit(event.EXIT, nil)
	log.Infof("exit success")
}

func checkNetworkProtocol(p string) string {
	switch p {
	case "ipv4":
		return "tcp4"
	case "ipv6":
		return "tcp6"
	}

	return "tcp"
}

func asyncLogCleaner(cleanPeriod, expiration time.Duration) (close chan struct{}) {
	t := time.NewTicker(cleanPeriod)
	close = make(chan struct{})
	go func() {
		for {
			select {
			case <-t.C:
				mgo.CleanupLogs(expiration)
			case <-close:
				return
			}
		}
	}()

	return
}