'''
Basic SQL database interface for minimal IGSN metadata.

All dates are stored in UTC.
'''
import logging
import datetime
import dateparser
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, UnicodeText, JSON, Float
from sqlalchemy.orm import relationship, exc
import sqlalchemy.exc
import sickle.oaiexceptions
import igsn_lib
from . import oai
import json

_L = logging.getLogger("igsn_lib.models")

Base = declarative_base()

class IGSN(Base):
    __tablename__ = 'igsn'
    id = Column(String, primary_key=True, doc="id is the IGSN value, must be unique in the datastore")
    oai_id = Column(String, doc="The OAI-PMH record id.")
    service_id = Column(Integer, ForeignKey('service.id'))
    record = relationship("Service", back_populates="records")
    harvest_time = Column(DateTime(timezone=True), default=igsn_lib.dtnow, doc="when the record was harvested")
    oai_time = Column(DateTime(timezone=True), nullable=True, doc="Timestamp reported for OAI-PMH record")
    igsn_time = Column(DateTime(timezone=True), nullable=True, doc="time reported in the IGSN record submitted or registered log event")
    registrant = Column(String, nullable=True, doc="registrant name reported in the IGSN record")
    related = Column(JSON, nullable=True, default=None, doc="related identifiers in the IGSN record")
    log = Column(JSON, nullable=True, default=None, doc="log entries in IGSN record")

    def __repr__(self):
        return json.dumps(self.asJsonDict(), indent=2)

    def asJsonDict(self):
        d = {
            'id':self.id,
            'oai_id': self.oai_id,
            'service_id':self.service_id,
            'harvest_time':None,
            'oai_time': None,
            'igsn_time': None,
            'registrant': self.registrant,
            'related': self.related,
            'log':self.log,
        }
        if self.harvest_time is not None:
            d['harvest_time'] = self.harvest_time.strftime(igsn_lib.JSON_TIME_FORMAT)
        if self.oai_time is not None:
            d['oai_time'] = self.oai_time.strftime(igsn_lib.JSON_TIME_FORMAT)
        if self.igsn_time is not None:
            d['igsn_time'] = self.igsn_time.strftime(igsn_lib.JSON_TIME_FORMAT)
        return d

    def fromOAIRecord(self, record):
        data = oai.oaiRecordToDict(record)
        self.id = data['igsn_id']
        self.oai_id = data['oai_id']
        self.oai_time = data['oai_time']
        self.registrant = data['registrant']
        self.igsn_time = data['igsn_time']
        if len(data['related']) > 0:
            self.related = data['related']
        if len(data['log']) > 0:
            self.log = data['log']


class Job(Base):
    __tablename__ = 'job'
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('service.id'))
    service = relationship('Service', back_populates='jobs')
    tstart = Column(DateTime(timezone=True), nullable=True, default=None)
    tend = Column(DateTime(timezone=True), nullable=True, default=None)
    ignore_deleted = Column(Boolean, default=True)
    metadata_prefix = Column(String, default='igsn')
    setspec = Column(String, nullable=True)
    tfrom = Column(
        DateTime(timezone=True),
        nullable=True,
        doc="start of time range for this job, OAI time range boundaries are inclusive"
    )
    tuntil = Column(
        DateTime(timezone=True),
        nullable=True,
        doc="end of time range for this job, OAI time range boundaries are inclusive"
    )
    resumption_token = Column(
        String,
        nullable=True,
        doc="resume token to retrieve the next page form the provider"
    )

    def getResumptionToken(self):
        if self.resumption_token is not None:
            tok = json.loads(self.resumption_token)
            return tok['token']
        return None


    def execute(self, session, callback=None, resume=True):
        svc = oai.getSickle(self.service.url)
        kwargs = {
            'metadataPrefix': self.metadata_prefix
        }
        if self.setspec is not None:
            kwargs['setSpec'] = self.setspec
        if self.tfrom is not None:
            kwargs['from'] = self.tfrom.strftime(igsn_lib.OAI_TIME_FORMAT)
        if self.tuntil is not None:
            kwargs['until'] = self.tuntil.strftime(igsn_lib.OAI_TIME_FORMAT)
        self.tstart = igsn_lib.dtnow()
        counter = 0
        try:
            records = svc.ListRecords(ignore_deleted=self.ignore_deleted, **kwargs)
        except sickle.oaiexceptions.NoRecordsMatch as e:
            _L.warning("No records found for job.id = %s", self.id)
            self.tend = igsn_lib.dtnow()
            session.commit()
            return counter
        for record in records:
            restok = records.resumption_token.token
            if restok != self.getResumptionToken():
                rtok = {'token':records.resumption_token.token,
                        'cursor':records.resumption_token.cursor,
                        'complete_list_size':records.resumption_token.complete_list_size,
                        'expiration_date': records.resumption_token.expiration_date}
                self.resumption_token = json.dumps(rtok)
                session.commit()
            try:
                igsn = IGSN(
                    service_id=self.service_id,
                    harvest_time=igsn_lib.dtnow()
                )
                igsn.fromOAIRecord(record.raw)
                _L.debug(igsn)
                exists = session.query(IGSN).get(igsn.id)
                if not exists:
                    _L.debug("NEW")
                    if callback is not None:
                        callback(record, igsn)
                    counter += 1
                    try:
                        session.add(igsn)
                        session.commit()
                    except sqlalchemy.exc.IntegrityError as e:
                        _L.warning('IGSN entry already exists: %s', str(igsn))
                else:
                    _L.debug("EXISTING")
            except Exception as e:
                _L.error(e)
        self.tend = igsn_lib.dtnow()
        session.commit()
        return counter



class Service(Base):
    __tablename__ = 'service'
    id = Column(Integer, primary_key=True)
    url = Column(String, nullable=False)
    tearliest = Column(DateTime(timezone=True), nullable=True)
    name = Column(UnicodeText, nullable=True)
    admin_email = Column(String, nullable=True)

    def __repr__(self):
        d = self.asJsonDict()
        return json.dumps(d, indent=2)

    def asJsonDict(self):
        d = {
            'id':self.id,
            'url':self.url,
            'tearliest': None,
            'name': self.name,
            'admin_email': self.admin_email
        }
        if self.tearliest is not None:
            d['tearliest'] = self.tearliest.strftime(igsn_lib.JSON_TIME_FORMAT)
        return d

    def populate(self, session=None, url=None):
        '''
        Populate record from Identify service

        If session is provided then record is commited on success

        Args:
            session: database session
            url: option url to use for base_url

        Returns: nothing
        '''
        if not url is None:
            self.url = url
        info = oai.identify(self.url)
        self.name = info.repositoryName
        self.tearliest = dateparser.parse(
            info.earliestDatestamp,
            settings={
                'TIMEZONE': '+0000',
                'RETURN_AS_TIMEZONE_AWARE': True
            })
        self.admin_email = info.adminEmail
        if session is not None:
            session.commit()

    def mostRecentRecordRetrieved(self, session):
        '''
        Get the most recent harvested record for this service

        Returns: IGSN
        '''
        rec = session.query(IGSN) \
            .join(Service) \
            .filter(Service.id == self.id) \
            .order_by(IGSN.oai_time.desc()) \
            .first()
        return rec

    def topupHarvestJob(self,
                        session,
                        ignore_deleted=True,
                        metadata_prefix='igsn',
                        setspec=None):
        last_rec = self.mostRecentRecordRetrieved(session)
        logging.debug("RECENT RECORD: %s", last_rec)
        job = self.createJob(
            session=session,
            ignore_deleted=ignore_deleted,
            metadata_prefix=metadata_prefix,
            setspec=setspec,
            tfrom = last_rec.oai_time
        )
        self.jobs.append(job)
        session.commit()
        return job


    def createJob(self,
                  session=None,
                  ignore_deleted=True,
                  metadata_prefix='igsn',
                  setspec=None,
                  tfrom=None,
                  tuntil=None):
        job = Job(
            service_id=self.id,
            ignore_deleted=ignore_deleted,
            metadata_prefix=metadata_prefix,
            setspec=setspec,
            tfrom=tfrom,
            tuntil=tuntil)
        self.jobs.append(job)
        if session is not None:
            session.commit()
        return job

    def createJobPackage(self,
                         session=None,
                         tfrom=None,
                         tuntil=None,
                         tdelta=365):
        '''
        Generates a set of jobs to do a bunch of harvesting

        Args:
            session:
            tfrom:
            tuntil:
            tdelta:

        Returns:

        '''
        pass


Service.jobs = relationship(
    'Job',
    order_by=Job.id,
    back_populates='service'
)

Service.records = relationship(
    'IGSN',
    order_by = IGSN.id,
    back_populates = 'record'
)

def createAll(engine):
    Base.metadata.create_all(engine)

def getOrCreate(
        session,
        model,
        create_method='',
        create_method_kwargs=None,
        **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except exc.NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.flush()
            return created, True
        except sqlalchemy.exc.IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False

def addService(session, url):
    svc,_ = getOrCreate(session, Service, url=url)
    if svc.tearliest is None:
        svc.populate(session=session)
    return svc