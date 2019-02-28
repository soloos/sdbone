#!/bin/bash
python ./init_hkvtable_op.py >          hkvtable_op.go
gofmt         hkvtable_op.go >          hkvtable_op.tmp.go
mv            hkvtable_op.tmp.go        hkvtable_op.go
python ./init_hkvtable_object.py >      hkvtable_object.go
gofmt         hkvtable_object.go >      hkvtable_object.tmp.go
mv            hkvtable_object.tmp.go    hkvtable_object.go
