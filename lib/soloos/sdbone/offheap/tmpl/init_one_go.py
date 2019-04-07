#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import sys

tmplGoFile = sys.argv[1]
keyName = sys.argv[2]
keyType = sys.argv[3]

content = ""
with open(tmplGoFile, "r") as f:
    content = f.read()
content = content.replace("MagicKeyName", keyName)
content = content.replace("MagicKeyType", keyType)

print content
