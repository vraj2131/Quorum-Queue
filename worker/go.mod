module github.com/forge/worker

go 1.26

require (
	github.com/forge/shared v0.0.0
	github.com/google/uuid v1.6.0
)

require github.com/lib/pq v1.10.9 // indirect

replace github.com/forge/shared => ../shared
