package mgo

import (
	"github.com/butalso/cronsun/common/db"
	"gopkg.in/mgo.v2"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/butalso/cronsun/common/log"
)

const (
	Coll_JobLog       = "job_log"
	Coll_JobLatestLog = "job_latest_log"
	Coll_Stat         = "stat"
)

// 任务执行记录
type JobLog struct {
	Id        bson.ObjectId `bson:"_id,omitempty" json:"id"`
	JobId     string        `bson:"jobId" json:"jobId"`               // 任务 Id，索引
	JobGroup  string        `bson:"jobGroup" json:"jobGroup"`         // 任务分组，配合 Id 跳转用
	User      string        `bson:"user" json:"user"`                 // 执行此次任务的用户
	Name      string        `bson:"name" json:"name"`                 // 任务名称
	Node      string        `bson:"node" json:"node"`                 // 运行此次任务的节点 id，索引
	Hostname  string        `bson:"hostname" json:"hostname"`         // 运行此次任务的节点主机名称，索引
	IP        string        `bson:"ip" json:"ip"`                     // 运行此次任务的节点主机IP，索引
	Command   string        `bson:"command" json:"command,omitempty"` // 执行的命令，包括参数
	Output    string        `bson:"output" json:"output,omitempty"`   // 任务输出的所有内容
	Success   bool          `bson:"success" json:"success"`           // 是否执行成功
	BeginTime time.Time     `bson:"beginTime" json:"beginTime"`       // 任务开始执行时间，精确到毫秒，索引
	EndTime   time.Time     `bson:"endTime" json:"endTime"`           // 任务执行完毕时间，精确到毫秒
	Cleanup   time.Time     `bson:"cleanup,omitempty" json:"-"`       // 日志清除时间标志
}

func EnsureJobLogIndex() {
	db.GetDB().WithC(Coll_JobLog, func(c *mgo.Collection) error {
		c.EnsureIndex(mgo.Index{
			Key: []string{"beginTime"},
		})
		c.EnsureIndex(mgo.Index{
			Key: []string{"hostname"},
		})
		c.EnsureIndex(mgo.Index{
			Key: []string{"ip"},
		})

		return nil
	})
}

type JobLatestLog struct {
	JobLog   `bson:",inline"`
	RefLogId string `bson:"refLogId,omitempty" json:"refLogId"`
}

func GetJobLogById(id bson.ObjectId) (l *JobLog, err error) {
	err = db.GetDB().FindId(Coll_JobLog, id, &l)
	return
}

var selectForJobLogList = bson.M{"command": 0, "output": 0}

func GetJobLogList(query bson.M, page, size int, sort string) (list []*JobLog, total int, err error) {
	err = db.GetDB().WithC(Coll_JobLog, func(c *mgo.Collection) error {
		total, err = c.Find(query).Count()
		if err != nil {
			return err
		}
		return c.Find(query).Select(selectForJobLogList).Sort(sort).Skip((page - 1) * size).Limit(size).All(&list)
	})
	return
}

func GetJobLatestLogList(query bson.M, page, size int, sort string) (list []*JobLatestLog, total int, err error) {
	err = db.GetDB().WithC(Coll_JobLatestLog, func(c *mgo.Collection) error {
		total, err = c.Find(query).Count()
		if err != nil {
			return err
		}
		return c.Find(query).Select(selectForJobLogList).Sort(sort).Skip((page - 1) * size).Limit(size).All(&list)
	})
	return
}

func GetJobLatestLogListByJobIds(jobIds []string) (m map[string]*JobLatestLog, err error) {
	var list []*JobLatestLog

	err = db.GetDB().WithC(Coll_JobLatestLog, func(c *mgo.Collection) error {
		return c.Find(bson.M{"jobId": bson.M{"$in": jobIds}}).Select(selectForJobLogList).Sort("beginTime").All(&list)
	})
	if err != nil {
		return
	}

	m = make(map[string]*JobLatestLog, len(list))
	for i := range list {
		m[list[i].JobId] = list[i]
	}
	return
}

type StatExecuted struct {
	Total     int64  `bson:"total" json:"total"`
	Successed int64  `bson:"successed" json:"successed"`
	Failed    int64  `bson:"failed" json:"failed"`
	Date      string `bson:"date" json:"date"`
}

func JobLogStat() (s *StatExecuted, err error) {
	err = db.GetDB().FindOne(Coll_Stat, bson.M{"name": "job"}, &s)
	return
}

func JobLogDailyStat(begin, end time.Time) (ls []*StatExecuted, err error) {
	const oneDay = time.Hour * 24
	err = db.GetDB().WithC(Coll_Stat, func(c *mgo.Collection) error {
		dateList := make([]string, 0, 8)

		cur := begin
		for {
			dateList = append(dateList, cur.Format("2006-01-02"))
			cur = cur.Add(oneDay)
			if cur.After(end) {
				break
			}
		}
		return c.Find(bson.M{"name": "job-day", "date": bson.M{"$in": dateList}}).Sort("date").All(&ls)
	})

	return
}

func CleanupLogs(expiration time.Duration) {
	err := db.GetDB().WithC(Coll_JobLog, func(c *mgo.Collection) error {
		_, err := c.RemoveAll(bson.M{"$or": []bson.M{
			bson.M{"$and": []bson.M{
				bson.M{"cleanup": bson.M{"$exists": true}},
				bson.M{"cleanup": bson.M{"$lte": time.Now()}},
			}},
			bson.M{"$and": []bson.M{
				bson.M{"cleanup": bson.M{"$exists": false}},
				bson.M{"endTime": bson.M{"$lte": time.Now().Add(-expiration)}},
			}},
		}})

		return err
	})

	if err != nil {
		log.Errorf("[Cleaner] Failed to remove expired logs: %s", err.Error())
		return
	}

}