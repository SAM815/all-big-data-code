#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    movie_id, user_id, rating = line.split(',')
    print('%s\t%s' % (user_id, rating))