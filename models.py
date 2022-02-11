from sqlalchemy import Column, Float, Integer, String, DateTime, ForeignKey
from base import Base
from sqlalchemy.orm import relationship
                                 

class Coins(Base):
    __tablename__ = "Coins"

    id = Column(Integer, primary_key=True)
    name = Column('Name', String)
    prices = relationship("Prices")

    def __init__(self, name):
        self.name = name


class Prices(Base):
    __tablename__ = "Prices"

    id = Column(Integer, primary_key=True)
    coin_id = Column(Integer, ForeignKey('Coins.id'), nullable=False)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    interval = Column('Interval', String)
    open = Column('Open', Float)
    high = Column('High', Float)
    low = Column('Low', Float)
    close = Column('Close', Float)
    volume = Column('Volume', Float)

    def __init__(self, coin_id, time_stamp, date, interval,
                    open, high, low, close, 
                    volume):
        self.coin_id = coin_id
        self.time_stamp = time_stamp
        self.date = date
        self.interval = interval
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


class FearGreed(Base):
    __tablename__ = "FearGreed"

    id = Column(Integer, primary_key=True)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    value = Column('Value', Integer)

    def __init__(self, time_stamp, date, value):
        self.time_stamp = time_stamp
        self.date = date
        self.value = value


class BurnedDC(Base):
    __tablename__ = "BurnedDC"

    id = Column(Integer, primary_key=True)
    time_stamp = Column('timestamp', Integer)
    date = Column('Date', DateTime)
    value = Column('Value', Integer)

    def __init__(self, time_stamp, date, value):
        self.time_stamp = time_stamp
        self.date = date
        self.value = value
