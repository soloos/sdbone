#!/usr/bin/env python2
# -*- coding: utf-8 -*-

ret = '''package offheap

import (
    "unsafe"
)

'''

schemaKeyTypes = [
        {'keyName': 'String', 'keyType': 'string'},
        {'keyName': 'Int32', 'keyType': 'int32'},
        {'keyName': 'Int64', 'keyType': 'int64'},
        {'keyName': 'Bytes12', 'keyType': '[12]byte'},
        {'keyName': 'Bytes64', 'keyType': '[64]byte'},
        ]

tmplRow = '''
type HKVTableObjectUPtrBy{keyName} uintptr

func (u HKVTableObjectUPtrBy{keyName}) Ptr() *HKVTableObjectBy{keyName} {{
    return (*HKVTableObjectBy{keyName}) (unsafe.Pointer(u))
}}

type HKVTableObjectBy{keyName} struct {{
    HSharedPointer
    Key {keyType}
}}
'''

for row in schemaKeyTypes:
    ret += tmplRow.format(keyName=row['keyName'], keyType=row['keyType'])

print ret
