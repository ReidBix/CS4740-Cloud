#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()	
	# split the line into words
	words = line.split(',')
	hour = int(words[1].split(':')[0])
	if hour >= 9 and hour <= 17:
		if words[6] == "10":
			actual = int(words[3])
			print '%s\t%s' % (hour, actual)
