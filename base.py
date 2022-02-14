from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from os import path
import statics


dbPath = '{0}/{1}'.format(path.dirname(path.abspath(__file__)),
                                    '{0}.db'.format(statics.DB_FILE_PATH))

engine = create_engine('sqlite:///{0}'.format(dbPath), echo=True)

print('SQLalchemy Engine created')

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()

Base = declarative_base()

