
import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, REAL, DateTime, Sequence, MetaData, create_engine
from sqlalchemy.orm import sessionmaker


#-------------------------------------Dependencies and database link-------------------------------------------

engine = create_engine('sqlite:///test.db', echo=True) # Link the database to the SQLAlchemy engine
Session = sessionmaker(bind=engine)
Base = declarative_base()
metadata = MetaData()
session = Session()


#--------------------------------------------------------------------------------------------------------------

#-------------------------------------Creating tables for database--------------------------------------------

def now():
    return datetime.datetime.now().isoformat().split(".")[0]

class Locations(Base):  # Create 'locations' table (NOT IMPLEMENTED YET)
    __tablename__ = 'locations'

    id = Column(Integer, Sequence('locations_id_seq'), primary_key=True)
    tsp = Column(Integer())
    tsp_name = Column(String(50))


class Scheduled(Base): # Create 'scheduled_qty' table
    __tablename__ = 'scheduled_qty'

    id = Column(Integer, Sequence('schedule_id_seq'), primary_key=True)
    tsp = Column(Integer())
    tsp_name = Column(String(50))
    loc = Column(String(50))
    loc_name = Column(String(50))
    gas_day = Column(String(50))
    cycle = Column(String(50))
    sched_qty = Column(Integer())
    qty_avail = Column(Integer())
    design_cap = Column(Integer())
    oper_cap = Column(Integer())
    posting_dt = Column(String(50))
    timestamp = Column(String(50), default=now())


class GasComp(Base): # Create 'gas_composition' table
    __tablename__ = 'gas_composition'

    id = Column(Integer, Sequence('gascomp_id_seq'), primary_key=True)
    timestamp = Column(String(50), default=now())

#--------------------------------------------------------------------------------------------------------------

#-------Runs the program-----------

if __name__ == '__main__':
    Base.metadata.create_all(engine)
    session.commit()


    for i in range(10):                            # test code to create a bunch of records
        data = Scheduled(tsp=100000, sched_qty=i)
        session.add(data)
    session.commit()
    print(datetime.datetime.now())
