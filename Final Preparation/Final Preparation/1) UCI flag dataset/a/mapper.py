import sys

for line in sys.stdin:
	line = line.strip()
	line = line.split(',')
	print('%s\t%s' %(line[0],line[18]))