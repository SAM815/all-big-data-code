#!/usr/bin/env python3
import sys

maxPrice = {}
car = None
max_price = 0

for line in sys.stdin:
        line= line.strip()
        
        key, make,doors,price= line.split('\t')

        try:
                price = float(price)
        except ValueError:
                price = float(0)
        
        if doors == 'two':
                try:
                        if price > maxPrice[make]:
                                maxPrice[make] = price
                except:
                        maxPrice[make] = price

                      

for key in maxPrice.keys():
        if maxPrice[key] > max_price:
                max_price = maxPrice[key]
                car = key

print('%s\t%s' %(car,max_price))
