#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    print("%s\t%s\t%s\t%s" %(1, line[2], line[5], line[25]))