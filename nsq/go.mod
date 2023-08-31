module github.com/nextmicro/next-component/nsq

go 1.21.0

require (
	github.com/nextmicro/next v0.0.0-20230618173759-f2295e0e2524
	github.com/nsqio/go-nsq v1.1.0
)

require github.com/golang/snappy v0.0.4 // indirect

replace github.com/nextmicro/next => ../../next
