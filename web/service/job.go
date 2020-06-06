package service

import (
	"encoding/json"
	"fmt"
	"github.com/butalso/cronsun/common/db"
	"github.com/butalso/cronsun/common/etcd"
	"github.com/butalso/cronsun/web"
	"github.com/butalso/cronsun/web/dal/mgo"
	"net/http"
	"sort"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/gorilla/mux"

	"github.com/butalso/cronsun/common/conf"
	"github.com/butalso/cronsun/common/log"
)

type Job struct{}

func (j *Job) GetJob(ctx *Context) {
	vars := mux.Vars(ctx.R)
	job, err := mgo.GetJob(vars["group"], vars["id"])
	var statusCode int
	if err != nil {
		if err == db.ErrNotFound {
			statusCode = http.StatusNotFound
		} else {
			statusCode = http.StatusInternalServerError
		}
		web.outJSONWithCode(ctx.W, statusCode, err.Error())
		return
	}

	web.outJSON(ctx.W, job)
}

func (j *Job) DeleteJob(ctx *Context) {
	vars := mux.Vars(ctx.R)
	_, err := mgo.DeleteJob(vars["group"], vars["id"])
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	web.outJSONWithCode(ctx.W, http.StatusNoContent, nil)
}

func (j *Job) ChangeJobStatus(ctx *Context) {
	job := &mgo.Job{}
	decoder := json.NewDecoder(ctx.R.Body)
	err := decoder.Decode(&job)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, err.Error())
		return
	}
	ctx.R.Body.Close()

	vars := mux.Vars(ctx.R)
	job, err = j.updateJobStatus(vars["group"], vars["id"], job.Pause)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	web.outJSON(ctx.W, job)
}

func (j *Job) updateJobStatus(group, id string, isPause bool) (*mgo.Job, error) {
	originJob, rev, err := etcd.GetJobAndRev(group, id)
	if err != nil {
		return nil, err
	}

	if originJob.Pause == isPause {
		return nil, err
	}

	originJob.Pause = isPause
	b, err := json.Marshal(originJob)
	if err != nil {
		return nil, err
	}

	_, err = etcd.DefalutClient.PutWithModRev(originJob.Key(), string(b), rev)
	if err != nil {
		return nil, err
	}

	return originJob, nil
}

func (j *Job) BatchChangeJobStatus(ctx *Context) {
	var jobIds []string
	decoder := json.NewDecoder(ctx.R.Body)
	err := decoder.Decode(&jobIds)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, err.Error())
		return
	}
	ctx.R.Body.Close()

	vars := mux.Vars(ctx.R)
	op := vars["op"]
	var isPause bool
	switch op {
	case "pause":
		isPause = true
	case "start":
	default:
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, "Unknow batch operation.")
		return
	}

	var updated int
	for i := range jobIds {
		id := strings.Split(jobIds[i], "/") // [Group, ID]
		if len(id) != 2 || id[0] == "" || id[1] == "" {
			continue
		}

		_, err = j.updateJobStatus(id[0], id[1], isPause)
		if err != nil {
			continue
		}
		updated++
	}

	web.outJSON(ctx.W, fmt.Sprintf("%d of %d updated.", updated, len(jobIds)))
}

func (j *Job) UpdateJob(ctx *Context) {
	var job = &struct {
		*cronsun.Job
		OldGroup string `json:"oldGroup"`
	}{}

	decoder := json.NewDecoder(ctx.R.Body)
	err := decoder.Decode(&job)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, err.Error())
		return
	}
	ctx.R.Body.Close()

	if err = job.Check(); err != nil {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, err.Error())
		return
	}

	var deleteOldKey string
	var successCode = http.StatusOK
	if len(job.ID) == 0 {
		successCode = http.StatusCreated
		job.ID = cronsun.NextID()
	} else {
		job.OldGroup = strings.TrimSpace(job.OldGroup)
		if job.OldGroup != job.Group {
			deleteOldKey = cronsun.JobKey(job.OldGroup, job.ID)
		}
	}

	b, err := json.Marshal(job)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	// remove old key
	// it should be before the put method
	if len(deleteOldKey) > 0 {
		if _, err = cronsun.DefalutClient.Delete(deleteOldKey); err != nil {
			log.Errorf("failed to remove old job key[%s], err: %s.", deleteOldKey, err.Error())
			web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
			return
		}
	}

	_, err = cronsun.DefalutClient.Put(job.Key(), string(b))
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	web.outJSONWithCode(ctx.W, successCode, nil)
}

func (j *Job) GetGroups(ctx *Context) {
	resp, err := cronsun.DefalutClient.Get(conf.Config.Cmd, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	var cmdKeyLen = len(conf.Config.Cmd)
	var groupMap = make(map[string]bool, 8)

	for i := range resp.Kvs {
		ss := strings.Split(string(resp.Kvs[i].Key)[cmdKeyLen:], "/")
		groupMap[ss[0]] = true
	}

	var groupList = make([]string, 0, len(groupMap))
	for k := range groupMap {
		groupList = append(groupList, k)
	}

	sort.Strings(groupList)
	web.outJSON(ctx.W, groupList)
}

func (j *Job) GetList(ctx *Context) {
	group := web.getStringVal("group", ctx.R)
	node := web.getStringVal("node", ctx.R)
	var prefix = conf.Config.Cmd
	if len(group) != 0 {
		prefix += group
	}

	type jobStatus struct {
		*cronsun.Job
		LatestStatus *cronsun.JobLatestLog `json:"latestStatus"`
		NextRunTime  string                `json:"nextRunTime"`
	}

	resp, err := cronsun.DefalutClient.Get(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	var nodeGroupMap map[string]*cronsun.Group
	if len(node) > 0 {
		nodeGrouplist, err := cronsun.GetNodeGroups()
		if err != nil {
			web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
			return
		}
		nodeGroupMap = map[string]*cronsun.Group{}
		for i := range nodeGrouplist {
			nodeGroupMap[nodeGrouplist[i].ID] = nodeGrouplist[i]
		}
	}

	var jobIds []string
	var jobList = make([]*jobStatus, 0, resp.Count)
	for i := range resp.Kvs {
		job := cronsun.Job{}
		err = json.Unmarshal(resp.Kvs[i].Value, &job)
		if err != nil {
			web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
			return
		}

		if len(node) > 0 && !job.IsRunOn(node, nodeGroupMap) {
			continue
		}
		jobList = append(jobList, &jobStatus{Job: &job})
		jobIds = append(jobIds, job.ID)
	}

	m, err := cronsun.GetJobLatestLogListByJobIds(jobIds)
	if err != nil {
		log.Errorf("GetJobLatestLogListByJobIds error: %s", err.Error())
	} else {
		for i := range jobList {
			jobList[i].LatestStatus = m[jobList[i].ID]
			nt := jobList[i].GetNextRunTime()
			if nt.IsZero() {
				jobList[i].NextRunTime = "NO!!"
			} else {
				jobList[i].NextRunTime = nt.Format("2006-01-02 15:04:05")
			}
		}
	}

	web.outJSON(ctx.W, jobList)
}

func (j *Job) GetJobNodes(ctx *Context) {
	vars := mux.Vars(ctx.R)
	job, err := cronsun.GetJob(vars["group"], vars["id"])
	var statusCode int
	if err != nil {
		if err == cronsun.ErrNotFound {
			statusCode = http.StatusNotFound
		} else {
			statusCode = http.StatusInternalServerError
		}
		web.outJSONWithCode(ctx.W, statusCode, err.Error())
		return
	}

	var nodes []string
	var exNodes []string
	groups, err := cronsun.GetGroups("")
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	for i := range job.Rules {
		inNodes := append(nodes, job.Rules[i].NodeIDs...)
		for _, gid := range job.Rules[i].GroupIDs {
			if g, ok := groups[gid]; ok {
				inNodes = append(inNodes, g.NodeIDs...)
			}
		}
		exNodes = append(exNodes, job.Rules[i].ExcludeNodeIDs...)
		inNodes = SubtractStringArray(inNodes, exNodes)
		nodes = append(nodes, inNodes...)
	}

	web.outJSON(ctx.W, UniqueStringArray(nodes))
}

func (j *Job) JobExecute(ctx *Context) {
	vars := mux.Vars(ctx.R)
	group := strings.TrimSpace(vars["group"])
	id := strings.TrimSpace(vars["id"])
	if len(group) == 0 || len(id) == 0 {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, "Invalid job id or group.")
		return
	}

	node := web.getStringVal("node", ctx.R)
	err := cronsun.PutOnce(group, id, node)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	web.outJSONWithCode(ctx.W, http.StatusNoContent, nil)
}

func (j *Job) GetExecutingJob(ctx *Context) {
	opt := &ProcFetchOptions{
		Groups:  web.getStringArrayFromQuery("groups", ",", ctx.R),
		NodeIds: web.getStringArrayFromQuery("nodes", ",", ctx.R),
		JobIds:  web.getStringArrayFromQuery("jobs", ",", ctx.R),
	}

	gresp, err := cronsun.DefalutClient.Get(conf.Config.Proc, clientv3.WithPrefix())
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	var list = make([]*processInfo, 0, 8)
	for i := range gresp.Kvs {
		proc, err := cronsun.GetProcFromKey(string(gresp.Kvs[i].Key))
		if err != nil {
			log.Errorf("Failed to unmarshal Proc from key: %s", err.Error())
			continue
		}

		if !opt.Match(proc) {
			continue
		}

		val := string(gresp.Kvs[i].Value)
		var pv = &cronsun.ProcessVal{}
		err = json.Unmarshal([]byte(val), pv)
		if err != nil {
			log.Errorf("Failed to unmarshal ProcessVal from val: %s", err.Error())
			continue
		}
		proc.ProcessVal = *pv
		procInfo := &processInfo{
			Process: proc,
		}
		job, err := cronsun.GetJob(proc.Group, proc.JobID)
		if err == nil && job != nil {
			procInfo.JobName = job.Name
		} else {
			procInfo.JobName = proc.JobID
		}
		list = append(list, procInfo)
	}

	sort.Sort(ByProcTime(list))
	web.outJSON(ctx.W, list)
}

func (j *Job) KillExecutingJob(ctx *Context) {
	proc := &cronsun.Process{
		ID:     web.getStringVal("pid", ctx.R),
		JobID:  web.getStringVal("job", ctx.R),
		Group:  web.getStringVal("group", ctx.R),
		NodeID: web.getStringVal("node", ctx.R),
	}

	if proc.ID == "" || proc.JobID == "" || proc.Group == "" || proc.NodeID == "" {
		web.outJSONWithCode(ctx.W, http.StatusBadRequest, "Invalid process info.")
		return
	}

	procKey := proc.Key()
	resp, err := cronsun.DefalutClient.Get(procKey)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	if len(resp.Kvs) < 1 {
		web.outJSONWithCode(ctx.W, http.StatusNotFound, "Porcess not found")
		return
	}

	var procVal = &cronsun.ProcessVal{}
	err = json.Unmarshal(resp.Kvs[0].Value, &procVal)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}
	if procVal.Killed {
		web.outJSONWithCode(ctx.W, http.StatusOK, "Killing process")
		return
	}

	procVal.Killed = true
	proc.ProcessVal = *procVal
	str, err := proc.Val()
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	_, err = cronsun.DefalutClient.Put(procKey, str)
	if err != nil {
		web.outJSONWithCode(ctx.W, http.StatusInternalServerError, err.Error())
		return
	}

	web.outJSONWithCode(ctx.W, http.StatusOK, "Killing process")
}

type ProcFetchOptions struct {
	Groups  []string
	NodeIds []string
	JobIds  []string
}

func (opt *ProcFetchOptions) Match(proc *cronsun.Process) bool {
	if len(opt.Groups) > 0 && !InStringArray(proc.Group, opt.Groups) {
		return false
	}

	if len(opt.JobIds) > 0 && !InStringArray(proc.JobID, opt.JobIds) {
		return false

	}

	if len(opt.NodeIds) > 0 && !InStringArray(proc.NodeID, opt.NodeIds) {
		return false
	}

	return true
}

type processInfo struct {
	*cronsun.Process
	JobName string `json:"jobName"`
}

type ByProcTime []*processInfo

func (a ByProcTime) Len() int           { return len(a) }
func (a ByProcTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByProcTime) Less(i, j int) bool { return a[i].Time.After(a[j].Time) }
