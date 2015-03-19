from sqlalchemy import *
from sqlalchemy.orm import *
#from sqlalchemy import (create_engine, MetaData, Table, Column, Integer,
#    String, ForeignKey, Float, DateTime, event)
#from sqlalchemy.orm import sessionmaker, mapper, relationship
from sqlalchemy.ext.horizontal_shard import ShardedSession
#from sqlalchemy.sql import operators, visitors
import os
cwd = '/home/raziel/meWorkspace/Eclipse/SQLAlchemy/sources/'#os.getcwd()
print cwd
engine1 = create_engine('sqlite://')#/shard1.db')
engine2 = create_engine('sqlite://')#/shard2.db')

metadata = MetaData()

product_table = Table('product', metadata,
                      Column('sku', String(20), primary_key=True),
                      Column('msrp', Numeric))

metadata.create_all(bind=engine1)
metadata.create_all(bind=engine2)

class Product(object):
    def __init__(self, sku, msrp):
        self.sku = sku
        self.msrp = msrp
        pass
    def __repr__(self):
        return '<Product %s>' % self.sku
    
clear_mappers()
product_mapper = mapper(Product, product_table)

def shard_chooser(mapper, instance, clause=None):
        if mapper is not product_mapper:
            return 'odd'
        if (instance.sku and instance.sku[0].isdigit() and int(instance.sku[0]) % 2 == 0):
            return 'even'
        else:
            return 'odd'
        pass
    
def id_chooser(query, ident):
    if query.mapper is not product_mapper:
        return ['odd']
    if (ident \
        and ident[0].isdigit()
        and int(ident[0]) % 2 == 0):
        return ['even']
    return ['odd']
    

def query_chooser(query):
    return ['even', 'odd']

Session = sessionmaker(class_=ShardedSession)
session = Session(shard_chooser=shard_chooser,id_chooser=id_chooser,
query_chooser=query_chooser,shards=dict(even=engine1,odd=engine2))

products = [ Product('%d%d%d' % (i,i,i), 0.0) for i in range(10) ]
for p in products:
    session.add(p)
    pass

session.flush()

for row in engine1.execute(product_table.select()):
    print row
    pass

for row in engine2.execute(product_table.select()):
    print row
    pass

for row in engine1.execute(product_table.select()):
    print row
    pass

for row in engine2.execute(product_table.select()):
    print row
    pass
    
print session.query(Product).all()

