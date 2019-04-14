#!/bin/bash

arr=(
        'String  string   string'
        'Int32   int32    int32'
        'Int64   int64    int64'
        'Uint64  uint64  uint64'
        'Bytes12 [12]byte bytes12'
        'Bytes64 [64]byte bytes64'
        'Bytes68 [68]byte bytes68'
        )

len=${#arr[@]}
for((i=0;i<len;i+=1))
do
        item=(${arr[$i]})

         cat ./tmpl/hkvtable_keytype.go|\
             sed "s/MagicKeyName/${item[0]}/g"|\
             sed "s/MagicKeyType/${item[1]}/g"|\
             gofmt > ./hkvtable_${item[2]}.go

         cat ./tmpl/lkvtable_keytype.go|\
             sed "s/MagicKeyName/${item[0]}/g"|\
             sed "s/MagicKeyType/${item[1]}/g"|\
             gofmt > ./lkvtable_${item[2]}.go
done

python ./tmpl/init_kvtable_common.py |gofmt > ./kvtable_common.go
