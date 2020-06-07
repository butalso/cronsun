module github.com/butalso/cronsun/web

go 1.14

require (
	github.com/butalso/cronsun/common v0.0.0-20200606064249-a47c2e9f1855
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292
	github.com/coreos/etcd v3.3.9+incompatible
	github.com/go-gomail/gomail v0.0.0-20160411212932-81ebce5c23df
	github.com/gorilla/mux v1.7.4
	go.uber.org/zap v1.15.0
	google.golang.org/grpc/examples v0.0.0-20200605192255-479df5ea818c // indirect
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

replace github.com/butalso/cronsun/common v0.0.0-20200606064249-a47c2e9f1855 => E:/code/go/src/github.com/butalso/cronsun/common
