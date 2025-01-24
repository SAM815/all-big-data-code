#!/usr/bin/env python

import sys

user_ratings = {}

for line in sys.stdin:
    user, ratings_str = line.strip().split('\t')
    
    ratings = [int(rating) for rating in ratings_str.split(',')]
    
    total = sum(ratings)
    count = len(ratings)
    
    if user not in user_ratings:
        user_ratings[user] = (total, count)
    else:
        prev_total, prev_count = user_ratings[user]
        user_ratings[user] = (prev_total + total, prev_count + count)

for user, (total, count) in user_ratings.items():
    average = total / count
    print('%s\t%s' % (user, average))
