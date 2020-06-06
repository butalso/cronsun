package service

import (
	"github.com/butalso/cronsun/common/etcd"
	"github.com/butalso/cronsun/web/dal/mgo"
	"time"

	v3 "github.com/coreos/etcd/clientv3"

	"github.com/butalso/cronsun/common/conf"
)

type Info struct{}

func (inf *Info) Overview(ctx *Context) {
	var overview = struct {
		TotalJobs        int64                   `json:"totalJobs"`
		JobExecuted      *mgo.StatExecuted   `json:"jobExecuted"`
		JobExecutedDaily []*mgo.StatExecuted `json:"jobExecutedDaily"`
	}{}

	const day = 24 * time.Hour
	days := 7

	overview.JobExecuted, _ = mgo.JobLogStat()
	end := time.Now()
	begin := end.Add(time.Duration(1-days) * day)
	statList, _ := mgo.JobLogDailyStat(begin, end)
	list := make([]*mgo.StatExecuted, days)
	cur := begin

	for i := 0; i < days; i++ {
		date := cur.Format("2006-01-02")
		var se *mgo.StatExecuted

		for j := range statList {
			if statList[j].Date == date {
				se = statList[j]
				statList = statList[1:]
				break
			}
		}

		if se != nil {
			list[i] = se
		} else {
			list[i] = &mgo.StatExecuted{Date: date}
		}

		cur = cur.Add(day)
	}

	overview.JobExecutedDaily = list
	gresp, err := etcd.DefalutClient.Get(conf.Config.Cmd, v3.WithPrefix(), v3.WithCountOnly())
	if err == nil {
		overview.TotalJobs = gresp.Count
	}

	outJSON(ctx.W, overview)
}
