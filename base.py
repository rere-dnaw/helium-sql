from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from os import path


# dbPath = '{0}/{1}'.format(path.dirname(path.abspath(__file__)),
#                                     '{0}.db'.format(statics.DB_CREDENTIALS['database']))
dbPath = '{0}/{1}'.format(path.dirname(path.abspath(__file__)),
                                    '{0}.db'.format('newDB'))


#engine = create_engine('postgresql://{0}'.format(dbPath), echo=True)
engine = create_engine('sqlite:///{0}'.format(dbPath), echo=True)

print('SQLalchemy Engine created')

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()

Base = declarative_base()

