from sqlalchemy import Column, Float, Integer, String, DateTime
from base import Base
                                 

class BTC_1D(Base):
    __tablename__ = "BTC_1D"

    id = Column(Integer, primary_key=True)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    open_value = Column('Open', Float)
    high_value = Column('High', Float)
    low_value = Column('Low', Float)
    close_value = Column('Close', Float)
    volume_value = Column('Volume', Float)

    def __init__(self, time_stamp, date, open_value, 
                    high_value, low_value, close_value, 
                    volume_value):
        self.time_stamp = time_stamp
        self.date = date
        self.open_value = open_value
        self.high_value = high_value
        self.low_value = low_value
        self.close_value = close_value
        self.volume_value = volume_value


class BTC_1H(Base):
    __tablename__ = "BTC_1H"

    id = Column(Integer, primary_key=True)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    open_value = Column('Open', Float)
    high_value = Column('High', Float)
    low_value = Column('Low', Float)
    close_value = Column('Close', Float)
    volume_value = Column('Volume', Float)

    def __init__(self, time_stamp, date, open_value, 
                    high_value, low_value, close_value, 
                    volume_value):
        self.time_stamp = time_stamp
        self.date = date
        self.open_value = open_value
        self.high_value = high_value
        self.low_value = low_value
        self.close_value = close_value
        self.volume_value = volume_value


class BTC_4H(Base):
    __tablename__ = "BTC_4H"

    id = Column(Integer, primary_key=True)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    open_value = Column('Open', Float)
    high_value = Column('High', Float)
    low_value = Column('Low', Float)
    close_value = Column('Close', Float)
    volume_value = Column('Volume', Float)

    def __init__(self, time_stamp, date, open_value, 
                    high_value, low_value, close_value, 
                    volume_value):
        self.time_stamp = time_stamp
        self.date = date
        self.open_value = open_value
        self.high_value = high_value
        self.low_value = low_value
        self.close_value = close_value
        self.volume_value = volume_value