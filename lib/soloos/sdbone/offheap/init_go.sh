#!/bin/bash

arr=(
        'String  string   string'
        'Int32   int32    int32'
        'Int64   int64    int64'
        'Bytes12 [12]byte bytes12'
        'Bytes64 [64]byte bytes64'
        )

len=${#arr[@]}
for((i=0;i<len;i+=1))
do
        item=(${arr[$i]})

        python ./tmpl/init_one_go.py ./tmpl/hkvtable_keytype.go ${item[0]} ${item[1]} |gofmt > ./hkvtable_${item[2]}.go
done

python ./tmpl/init_hkvtable_common.py |gofmt > ./hkvtable_common.go
