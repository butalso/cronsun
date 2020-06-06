package main

import (
	"flag"
	"fmt"
	"github.com/butalso/cronsun/common/conf"
	"github.com/butalso/cronsun/common/db"
	"github.com/butalso/cronsun/common/etcd"
	"github.com/butalso/cronsun/web/service"
	slog "log"
	"net"
	"os"
	"time"

	"github.com/cockroachdb/cmux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"github.com/butalso/cronsun/common/event"
	"github.com/butalso/cronsun/common/log"
)

var (
	level    = flag.Int("l", 0, "log level, -1:debug, 0:info, 1:warn, 2:error")
	confFile = flag.String("conf", "conf/files/base.json", "config file path")
	network  = flag.String("network", "", "network protocol of listen address: ipv4/ipv6, or empty use both")
)

func main() {
	flag.Parse()

	lcf := zap.NewDevelopmentConfig()
	lcf.Level.SetLevel(zapcore.Level(*level))
	lcf.Development = false
	logger, err := lcf.Build(zap.AddCallerSkip(1))
	if err != nil {
		slog.Fatalln("new log err:", err.Error())
	}
	log.SetLogger(logger.Sugar())

	if err = cronsun.Init(*confFile, true); err != nil {
		log.Errorf(err.Error())
		return
	}
	service.EnsureJobLogIndex()

	l, err := net.Listen(checkNetworkProtocol(*network), conf.Config.Web.BindAddr)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	// Create a cmux.
	m := cmux.New(l)
	httpL := m.Match(cmux.HTTP1Fast())
	httpServer, err := web.InitServer()
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	if conf.Config.Mail.Enable {
		var noticer cronsun.Noticer

		if len(conf.Config.Mail.HttpAPI) > 0 {
			noticer = &cronsun.HttpAPI{}
		} else {
			mailer, err := cronsun.NewMail(30 * time.Second)
			if err != nil {
				log.Errorf(err.Error())
				return
			}
			noticer = mailer
		}
		go noticer.StartNoticer(noticer)
	}

	period := int64(conf.Config.Web.LogCleaner.EveryMinute)
	var stopCleaner func(interface{})
	if period > 0 {
		closeChan := service.RunLogCleaner(time.Duration(period)*time.Minute, time.Duration(conf.Config.Web.LogCleaner.ExpirationDays)*time.Hour*24)
		stopCleaner = func(i interface{}) {
			close(closeChan)
		}
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

var (
	_Uid int
)

func Init(baseConfFile string, watchConfiFile bool) (err error) {
	// init config
	if err = conf.Init(baseConfFile, watchConfiFile); err != nil {
		return fmt.Errorf("Init Config failed: %s", err)
	}

	// init etcd client
	if etcd.DefalutClient, err = etcd.NewClient(conf.Config); err != nil {
		return fmt.Errorf("Connect to ETCD %s failed: %s",
			conf.Config.Etcd.Endpoints, err)
	}

	// init mongoDB
	if db.mgoDB, err = db.NewMdb(conf.MgoConfig.Mgo); err != nil {
		return fmt.Errorf("Connect to MongoDB %s failed: %s",
			conf.Config.Mgo.Hosts, err)
	}

	_Uid = os.Getuid()

	initialized = true
	return
}
