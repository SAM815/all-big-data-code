#!/usr/bin/env python

import sys

movie_ratings = {}

for line in sys.stdin:
    movie_id, ratings_str = line.strip().split('\t')
    
    ratings = list(map(int, ratings_str.split(',')))
    
    total_rating = sum(ratings)
    rating_count = len(ratings)
    
    if movie_id not in movie_ratings:
        movie_ratings[movie_id] = (total_rating, rating_count)
    else:
        prev_total_rating, prev_rating_count = movie_ratings[movie_id]
        movie_ratings[movie_id] = (prev_total_rating + total_rating, prev_rating_count + rating_count)

for movie_id, (total_rating, rating_count) in movie_ratings.items():
    average_rating = total_rating / rating_count
    print('%s\t%s' % (movie_id, average_rating))
