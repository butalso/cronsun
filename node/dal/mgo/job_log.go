package mgo

import (
	"github.com/butalso/cronsun/common/conf"
	"github.com/butalso/cronsun/common/db"
	"gopkg.in/mgo.v2/bson"
	"time"
)

func CreateJobLog(j *node.Job, t time.Time, rs string, success bool) {
	et := time.Now()
	j.Avg(t, et)

	jl := JobLog{
		Id:    bson.NewObjectId(),
		JobId: j.ID,

		JobGroup: j.Group,
		Name:     j.Name,
		User:     j.User,

		Node:     j.RunOn,
		Hostname: j.Hostname,
		IP:       j.Ip,

		Command: j.Command,
		Output:  rs,
		Success: success,

		BeginTime: t,
		EndTime:   et,
	}

	if conf.Config.Web.LogCleaner.EveryMinute > 0 {
		var expiration int
		if j.LogExpiration > 0 {
			expiration = j.LogExpiration
		} else {
			expiration = conf.Config.Web.LogCleaner.ExpirationDays
		}
		jl.Cleanup = jl.EndTime.Add(time.Duration(expiration) * time.Hour * 24)
	}

	if err := db.GetDB().Insert(Coll_JobLog, jl); err != nil {
		log.Errorf(err.Error())
	}

	latestLog := &JobLatestLog{
		RefLogId: jl.Id.Hex(),
		JobLog:   jl,
	}
	latestLog.Id = ""
	if err := db.GetDB().Upsert(Coll_JobLatestLog, bson.M{"node": jl.Node, "hostname": jl.Hostname, "ip": jl.IP, "jobId": jl.JobId, "jobGroup": jl.JobGroup}, latestLog); err != nil {
		log.Errorf(err.Error())
	}

	var inc = bson.M{"total": 1}
	if jl.Success {
		inc["successed"] = 1
	} else {
		inc["failed"] = 1
	}

	err := db.GetDB().Upsert(Coll_Stat, bson.M{"name": "job-day", "date": time.Now().Format("2006-01-02")}, bson.M{"$inc": inc})
	if err != nil {
		log.Errorf("increase stat.job %s", err.Error())
	}
	err = db.GetDB().Upsert(Coll_Stat, bson.M{"name": "job"}, bson.M{"$inc": inc})
	if err != nil {
		log.Errorf("increase stat.job %s", err.Error())
	}
}
