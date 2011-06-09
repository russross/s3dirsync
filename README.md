Propolis
========

Propolis publishes a local directory to an Amazon S3 bucket. It
watches for changes in the local directory and mirrors them online.
This is useful as a publishing tool or as a simple backup system.

It is written in Go and uses sqlite as a local metadata cache. It
uses inotify to watch for local changes, and as such requires Linux.


Status
======

Early stages. There is not much point in trying this out yet, as it
is an early work-in-progress. I mostly needed a git repository so
that I could work on it from two locations.
