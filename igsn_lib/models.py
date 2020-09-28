'''

'''
import logging
import datetime
import dateparser
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, UnicodeText
from sqlalchemy.orm import relationship
import xmltodict
import sickle.oaiexceptions

import igsn_lib
from . import oai

_L = logging.getLogger("igsn_lib.models")

Base = declarative_base()

class IGSN(Base):
    __tablename__ = 'igsn'
    id = Column(String, primary_key=True, doc="id is the IGSN value, must be unique in the datastore")
    job_id = Column(Integer, ForeignKey('job.id'))
    entry = relationship("Job", back_populates="entries")
    tstamp = Column(DateTime, default=datetime.datetime.utcnow, doc="when the entry was harvested")
    raw = Column(UnicodeText, nullable=False, doc="raw xml response")
    oai_datestamp = Column(DateTime, nullable=True, doc="time reported in the oai datestamp")
    creator = Column(String, nullable=True, doc="creator reported in the dc:creator field")


class Job(Base):
    __tablename__ = 'job'
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('service.id'))
    service = relationship('Service', back_populates='jobs')
    tstart = Column(DateTime, default=datetime.datetime.utcnow)
    tend = Column(DateTime, nullable=True)
    ignore_deleted = Column(Boolean, default=True)
    metadata_prefix = Column(String, default='oai_dc')
    setspec = Column(String, nullable=True)
    tfrom = Column(
        DateTime,
        nullable=True,
        doc="start of time range for this job, OAI time range boundaries are inclusive"
    )
    tuntil = Column(
        DateTime,
        nullable=True,
        doc="end of time range for this job, OAI time range boundaries are inclusive"
    )
    resumption_token = Column(
        String,
        nullable=True,
        doc="resume token to retrieve the next page form the provider"
    )

    def execute(self, session, callback=None, resume=True):
        svc = oai.getSickle(self.service.url)
        kwargs = {
            'metadataPrefix': self.metadata_prefix
        }
        if self.setspec is not None:
            kwargs['setSpec'] = self.setspec
        if self.tfrom is not None:
            kwargs['from'] = self.tfrom
        if self.tuntil is not None:
            kwargs['until'] = self.tuntil
        counter = 0
        try:
            records = svc.ListRecords(ignore_deleted=self.ignore_deleted, **kwargs)
        except sickle.oaiexceptions.NoRecordsMatch as e:
            _L.warning("No records found for job.id = %s", self.id)
            return counter
        for record in records:
            if records.resumption_token != self.resumption_token:
                self.resumption_token = records.resumption_token
                session.commit()
            try:
                data = oai.oaiRecordToDict(record.raw)
                igsn = IGSN(
                    id = data['igsn_id'],
                    job_id = self.id,
                    raw = record.raw
                )
                igsn.creator = data['creator']
                igsn.oai_datestamp = data['tstamp']
                if callback is not None:
                    callback(record, igsn)
                counter += 1
                self.entries.add(igsn)
            except Exception as e:
                _L.error(e)
        return counter



class Service(Base):
    __tablename__ = 'service'
    id = Column(Integer, primary_key=True)
    url = Column(String, nullable=False)
    tearliest = Column(DateTime, nullable=True)
    name = Column(UnicodeText, nullable=True)
    admin_email = Column(String, nullable=True)

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
        self.tearliest = info.earliestDatestamp
        self.admin_email = info.adminEmail
        if session is not None:
            session.commit()

    def createJob(self,
                  session=None,
                  ignore_deleted=True,
                  metadata_prefix='oai_dc',
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

Job.entries = relationship(
    'IGSN',
    order_by = IGSN.id,
    back_populates = 'entry'
)

