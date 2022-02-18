from base import Session, engine, Base
from db_insert import db_insert_binance
from db_insert import db_insert_fear_greed
from db_insert import db_insert_DC
from db_insert import db_insert_inflationHNT


Base.metadata.create_all(engine)

session = Session()


def init_db():
    '''
    This method will initialize the database
    '''

    db_insert_binance.add_coin_price_data()

    db_insert_fear_greed.pull_fear_greed_data()

    db_insert_DC.pull_data_DC_burned()

    db_insert_inflationHNT.pull_HNT_inflation_data()


init_db()


session.commit()
session.close()