#!/usr/bin/python -O

f=open('output.txt','r')
g1=open('outputPitches.txt','w')
g2=open('outputRbis.txt','w')
g3=open('outputBases.txt','w')

reduced = []
while True:
  x=f.readline().split()
  if (len(x) is 0): break
  x = (x[0], x[1].split(","))
  reduced.append(x)

reducedPitches = sorted(reduced, key=lambda line: line[1][1], reverse=True)
reducedRbis = sorted(reduced, key=lambda line: line[1][2], reverse=True)
reducedBases = sorted(reduced, key=lambda line: line[1][3], reverse=True)

for item in reducedPitches:
    g1.write("%s: %s \n" % (item[0], item[1]))
for item in reducedRbis:
    g2.write("%s: %s \n" % (item[0], item[1]))
for item in reducedBases:
    g3.write("%s: %s \n" % (item[0], item[1]))
