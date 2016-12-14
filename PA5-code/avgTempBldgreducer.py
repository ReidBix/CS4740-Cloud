#!/usr/bin/env python

from operator import itemgetter
import sys

current_hour = None
current_temp = 0
temp_count = 1
hour = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    hour, temp = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        temp = int(temp)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_hour == hour:
	temp_count += 1
	current_temp += temp 
    else:
        if current_hour:
            # write result to STDOUT
            print '%s\t%s' % (current_hour, float(current_temp)/temp_count)
        current_temp = temp 
        current_hour = hour 
	temp_count = 1

# do not forget to output the last word if needed!
if current_hour == hour:
    print '%s\t%s' % (current_hour, float(current_temp)/temp_count)
