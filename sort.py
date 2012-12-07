#!/usr/bin/python -O

f=open('output.txt','r')
g=open('sortedOutput.txt','w')

reduced = []
while True:
  x=f.readline()
  if x is None: break
  x = x.split()
  x = (x[0], x[1])
  append(reduced, x.split())

sorted(reduced, key=lambda line: line[1])

for item in reduced:
  g.write("%s\n" % item)
