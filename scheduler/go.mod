module github.com/forge/scheduler

go 1.26

require (
	github.com/forge/shared v0.0.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	go.etcd.io/etcd/client/v3 v3.5.12
)

replace github.com/forge/shared => ../shared
