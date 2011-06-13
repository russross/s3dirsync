include $(GOROOT)/src/Make.inc

TARG=propolis
GOFILES=main.go s3.go cache.go queue.go sync.go

include $(GOROOT)/src/Make.cmd
