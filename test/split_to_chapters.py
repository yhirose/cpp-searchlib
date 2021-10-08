import sys
from itertools import groupby

lines = [line.rstrip().split('\t') for line in sys.stdin.readlines()]
for key, group in groupby(lines, key=lambda x: [int(x[1]), int(x[2])]):
    book, chap = key
    text = '\\n'.join([x[4] for x in group])
    print('{:d}{:02d}\t{}'.format(book, chap, text))
