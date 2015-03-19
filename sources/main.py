'''
Created on Apr 16, 2014

@author: raziel
'''
from sqliteShard import *

if __name__ == '__main__':
    for row in engine1.execute(bundesliga_table.select()):
        print row
        pass

    for row in engine2.execute(bundesliga_table.select()):
        print row
        pass
    
    for row in engine1.execute(bundesliga_table.select()):
        print row
        pass
    
    for row in engine2.execute(bundesliga_table.select()):
        print row
        pass
    print "done!"    
    print len(session.query(Bundesliga).all())
    pass


