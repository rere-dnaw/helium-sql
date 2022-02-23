from base import Session, engine, Base
import db_insert_binance
import db_insert_fear_greed
import db_insert_DC
import db_insert_rewardsHNT
import db_insert_HNT_supply


Base.metadata.create_all(engine)

session = Session()


def init_db():
    '''
    This method will initialize the database
    '''

    db_insert_binance.add_coin_price_data()

    db_insert_fear_greed.pull_fear_greed_data()

    db_insert_DC.pull_data_DC_burned()

    db_insert_rewardsHNT.pull_HNT_reward_data()

    db_insert_HNT_supply.pull_HNT_pull_supply_HNT()


init_db()


session.commit()
session.close()