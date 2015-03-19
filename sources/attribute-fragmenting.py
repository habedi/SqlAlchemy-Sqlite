from sqlalchemy import *
from sqlalchemy.orm import *
# from sqlalchemy import Table, MetaData, Column, ForeignKey, Integer, String

engine1 = create_engine('sqlite://')
engine2 = create_engine('sqlite://')
metadata = MetaData()
product_table = Table('product', metadata,
Column('sku', String(20), primary_key=True),
Column('msrp', Numeric))
product_summary_table = Table('product_summary', metadata,
Column('sku', None, ForeignKey('product.sku'), primary_key=True),
Column('name', Unicode(255)),
Column('description', Unicode))
product_table.create(bind=engine1)
product_summary_table.create(bind=engine2)

stmt = product_table.insert()
engine1.execute(stmt, 
                [dict(sku="123", msrp=12.34),
                 dict(sku="456", msrp=22.12),
                 dict(sku="789", msrp=41.44)])

stmt = product_summary_table.insert()

engine2.execute(stmt,
                [dict(sku="123", name="Shoes", description="Some Shoes"),
                 dict(sku="456", name="Pants", description="Some Pants"),
                 dict(sku="789", name="Shirts", description="Some Shirts")])

class Product(object):
    def __init__(self, sku, msrp, summary=None):
            self.sku = sku
            self.msrp = msrp
            self.summary = summary
            pass
        
    def __repr__(self):
        return '<Product %s>' % self.sku

class ProductSummary(object):
    def __init__(self, name, description):
        self.name = name
        self.description = description
        pass
    
    def __repr__(self):
        return '<ProductSummary %s>' % self.name

clear_mappers()
mapper(ProductSummary, product_summary_table,
       properties=dict(product=relation(Product,
                                        backref=backref('summary', uselist=False))))
mapper(Product, product_table)


# # TESTING 
# in console
Session = sessionmaker(binds={Product:engine1,ProductSummary:engine2})
session = Session()
engine1.echo = engine2.echo = True
print session.query(Product).all()

