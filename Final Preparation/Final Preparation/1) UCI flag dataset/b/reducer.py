# b) MapReduce code to find the average population in each landmass

import sys

average={}
count={}
total={}


for line in sys.stdin:
        line= line.strip()
        key, val= line.split('\t')
       
        
        
        try:
                average[key]= average[key]+float(val)
                
        except:
                average[key]= float(val[0])

        
        try:
                count[key]= count[key]+1
        except:
                count[key]= 1
                
        
for key in average.keys():
        total[key]= average[key]/count[key]
        print("%s\t%f" %(key,total[key]))
            
                
