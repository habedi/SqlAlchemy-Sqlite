'''
Created on Apr 17, 2014

@author: raziel
'''


l = []
def extract(filename):
    jump = True
    inf = open(filename,"r")
    for i in inf:
        if jump:
            x = i.strip().split(',')
            l.append((x[0],x[1],x[2],x[3],x[4],x[5]))
        else:
            jump = False
            pass
    inf.close()
    pass

files = ['D4','D5','D6','D7','D8','D9','D10','D11','D12']

for i in files:
    extract(i+'.csv')
    pass

print (l)
