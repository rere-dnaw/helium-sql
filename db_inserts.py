from datetime import date

from models import BTC_1D, BTC_1H, BTC_4H
from base import Session, engine, Base


Base.metadata.create_all(engine)

session = Session()



print('stop')




