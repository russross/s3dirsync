include $(GOROOT)/src/Make.inc

TARG=propolis
GOFILES=main.go s3.go cache.go

include $(GOROOT)/src/Make.cmd
