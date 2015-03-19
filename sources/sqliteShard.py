'''

Created on Apr 16, 2014
@author: raziel

'''

l = []
def extract(filename):
    jump = False
    inf = open(filename, "r")
    for i in inf:
        if jump:
            x = i.strip().split(',')
            l.append((x[0], x[1], x[2], x[3], x[4], x[5]))
        else:
            jump = True
            pass
    inf.close()
    pass

files = ['D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10', 'D11', 'D12']

for i in files:
    extract(i + '.csv')
    pass

from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date
from sqlalchemy.orm import clear_mappers, mapper, sessionmaker
from sqlalchemy.ext.horizontal_shard import ShardedSession
import os
import datetime

# ddir = '/mnt'
cdir = os.getcwd()
# print 'firebird+fdb://shardu:shardu@localhost:3050/'+ddir+"/DATA1/data/meShard1.fdb"
# engine1 = create_engine('firebird+fdb://shardu:shardu@localhost:3050/'+ddir+"/DATA1/data/meShard1.fdb")
# engine2 = create_engine('firebird+fdb://shardu:shardu@localhost:3050/'+ddir+"/DATA2/data/meShard2.fdb")
# engine3 = create_engine('firebird+fdb://shardu:shardu@localhost:3050/'+ddir+"/DATA3/data/meShard3.fdb")

engine1 = create_engine('sqlite:///' + cdir + '/shard1.db') 
engine2 = create_engine('sqlite:///' + cdir + '/shard2.db') 
engine3 = create_engine('sqlite:///' + cdir + '/shard3.db') 

metadata = MetaData()
bundesliga_table = Table('Bundesliga', metadata,
                      Column('index', String(20), primary_key=True),
                      Column('division', String(5), default='D1', nullable=False),
                      Column('date', Date), Column('home', String(30), nullable=False),
                      Column('away', String(30)), Column('home_goals', Integer, nullable=False),
                      Column('away_goals', Integer, nullable=False))

# # three shards
metadata.create_all(bind=engine1)
metadata.create_all(bind=engine2)
metadata.create_all(bind=engine3)

class Bundesliga(object):
    def __init__(self, index, division, date, home, away, home_goals, away_goals):
        self.index = index
        self.division = division
        self.date = date
        self.home = home
        self.away = away
        self.home_goals = home_goals
        self.away_goals = away_goals
        pass
    
    def __repr__(self):
        return '<Bundesliga (%s,%s,%s,%s,%s,%s,%s)>' % (self.index, self.division,
        self.date, self.home, self.away, self.home_goals, self.away_goals)
    
clear_mappers()
Bundesliga_mapper = mapper(Bundesliga, bundesliga_table)

def shard_chooser(mapper, instance, clause=None):
        if mapper is not bundesliga_table:
            return 'zero'
        if (instance.index and instance.index.isdigit() and (int(instance.index) % 3 == 1)):
            return 'one'
        if (instance.index and instance.index.isdigit() and (int(instance.index) % 3 == 2)):
            return 'two'
        return 'zero'
        pass
    
def id_chooser(query, ident):
    if query.mapper is not bundesliga_table:
        return ['zero']
    if (ident and ident[0].isdigit() and int(ident[0]) % 3 == 1):
        return ['one']
    if (ident and ident[0].isdigit() and int(ident[0]) % 3 == 2):
        return ['two']
    # print "not found"
    return ['zero']
    

def query_chooser(query):
    return ['zero', 'one' , 'two']

Session = sessionmaker(class_=ShardedSession)
session = Session(shard_chooser=shard_chooser, id_chooser=id_chooser,
query_chooser=query_chooser, shards=dict(zero=engine1, one=engine2, two=engine3))


def loadBunesliga():
    index = 0
    for i in l:
        d = i[1].split("/")
        # print d
        d = datetime.date(int(d[2]), int(d[1]), int(d[0]))
        # print (index,i[0],d,i[2],i[3],int(i[4]),int(i[5]))
        session.add(Bundesliga(str(index), i[0], d, i[2], i[3], int(i[4]), int(i[5])))
        index += 1
        pass

    session.flush()
    session.commit()
    pass
    

loadBunesliga()

# print (session.query(Bundesliga).all())
# print session.query(Bundesliga)

q = (session.query(Bundesliga).
     filter(Bundesliga.home == u'Dortmund'))

for i in  q.all():
    print i
    
session.close_all()

