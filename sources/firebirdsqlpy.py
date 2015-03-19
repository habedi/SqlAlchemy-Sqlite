import fdb
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm.exc import MultipleResultsFound
# from sqlalchemy import (create_engine, MetaData, Table, Column, Integer,
#    String, ForeignKey, Float, DateTime, event)
# from sqlalchemy.orm import sessionmaker, mapper, relationship
from sqlalchemy.ext.horizontal_shard import ShardedSession, ShardedQuery
# from sqlalchemy.sql import operators, visitors
import os
cwd = '/home/raziel/meWorkspace/Eclipse/SQLAlchemy/sources/'  # os.getcwd()
print cwd
engine1 = create_engine(
                        'firebird+fdb://shardu:shardu@localhost:3050//home/raziel/Big-Data/DBs/meDB1.fdb')
engine2 = create_engine(
                        'firebird+fdb://shardu:shardu@localhost:3050//home/raziel/Big-Data/DBs/meDB2.fdb')

metadata = MetaData()

product_table = Table('product', metadata,
                      Column('idx', String(20), primary_key=True),
                      Column('name', String(50)))

metadata.create_all(bind=engine1)
metadata.create_all(bind=engine2)

class Product(object):
    __tablename__ = 'product'
    def __init__(self, idx, name):
        self.idx = idx
        self.name = name
        pass
    def __repr__(self):
        return "<Product (idx='%s',name='%s')>" % (self.idx, self.name)
    
clear_mappers()
product_mapper = mapper(Product, product_table)

def shard_chooser(mapper, instance, clause=None):
        if mapper is not product_mapper:
            return 'odd'
        if (instance.idx and instance.idx[0].isdigit() and int(instance.idx) % 2 == 0):
            return 'even'
        else:
            return 'odd'
        pass
    
def id_chooser(query, ident):
    if query.mapper is not product_mapper:
        return ['odd']
    if (ident and ident[0].isdigit() and int(ident[0]) % 2 == 0):
        return ['even']
    return ['odd']
    

def query_chooser(query):
    return ['even', 'odd']

Session = sessionmaker(class_=ShardedSession)
session = Session(shard_chooser=shard_chooser, id_chooser=id_chooser,
query_chooser=query_chooser, shards=dict(even=engine1, odd=engine2))


# session.flush()
try: 
    print session.query(Product).filter(Product.idx==u'111').one()
except MultipleResultsFound, e:
    print e
#session.query(Product).filter(Product.idx == '111')

# print session.query(Product).filter_by(idx='111').one()
# .exc("select * from Product ")

# products = [ Product('%d%d%d' % (i,i,i), '0.0') for i in range(10) ]
# print products[0].__repr__()
# for p in products:
#     print session.add(p)
#     pass
# # commiting 
# session.commit()


# print session.flush()

# for row in engine1.execute(product_table.select()):
#     print row
#     pass
# 
# for row in engine2.execute(product_table.select()):
#     print row
#     pass

    
# session.query(Product).all()
