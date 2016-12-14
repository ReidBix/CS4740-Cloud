#!/usr/bin/env python

from operator import itemgetter
import sys

current_building = None
current_diff = 0
building = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    building, diff = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        diff = int(diff)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_building == building:
	current_diff += diff
    else:
        if current_building:
            # write result to STDOUT
            print '%s\t%s' % (current_building, current_diff)
        current_diff = diff 
        current_building = building

# do not forget to output the last word if needed!
if current_building == building:
    print '%s\t%s' % (current_building, current_diff)
