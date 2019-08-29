Propolis
========

Propolis publishes a local directory to an Amazon S3 bucket. It
watches for changes in the local directory and mirrors them online.
This is useful as a publishing tool or as a simple backup system.

It is written in Go and uses sqlite as a local metadata cache. It
uses inotify to watch for local changes, and as such requires Linux.


Status
======

Dead. I never got very far on this and don't expect to ever continue development.
