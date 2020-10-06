'''
Quick example of a harvest
'''

import logging
import dateutil.relativedelta
import dateutil.tz
import datetime
import igsn_lib.models
import igsn_lib.util
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pprint import pprint

def getEngine():
    pguser = "igsn_cache"
    pgpass = igsn_lib.util.getCredentials(pguser)
    pgdatabase = "igsn_01"
    engine = create_engine(f"postgresql+psycopg2://{pguser}:{pgpass}@localhost:5432/{pgdatabase}")
    igsn_lib.models.createAll(engine)
    return engine

def getSession(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def testJoins():
    logging.basicConfig(level=logging.DEBUG)
    engine = getEngine()
    session = getSession(engine)
    url = "http://doidb.wdc-terra.org/igsnoaip/oai"
    service = session.query(igsn_lib.models.Service)\
        .filter(igsn_lib.models.Service.url==url)\
        .first()
    if service is None:
        service = igsn_lib.models.addService(session, url)
    print(service)
    job = service.topupHarvestJob(session)
    job.execute(session)
    #print(f"{rec.id} {rec.oai_datestamp}")
    session.close()


def main():
    logging.basicConfig(level=logging.DEBUG)
    #engine = create_engine('sqlite:///igsn_store.db')
    engine = getEngine()
    session = getSession(engine)

    service = igsn_lib.models.addService(session, url="http://doidb.wdc-terra.org/igsnoaip/oai")
    pprint(service)
    pprint(service.jobs)

    tuntil = datetime.datetime.now(dateutil.tz.UTC) + dateutil.relativedelta.relativedelta(days=-1)
    tfrom = tuntil + dateutil.relativedelta.relativedelta(days=-365)
    job = service.createJob(
        session,
        setspec='IEDA',
        tfrom=tfrom,
        tuntil=tuntil
    )
    job.execute(session)
    session.close()

if __name__ == "__main__":
    #testJoins()
    main()
