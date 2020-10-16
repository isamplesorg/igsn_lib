"""
Basic SQL database interface for minimal IGSN metadata.

All dates are stored in UTC.
"""
import logging
import dateparser
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.exc
import sickle.oaiexceptions
import igsn_lib
import igsn_lib.oai
import igsn_lib.time
import json

_L = logging.getLogger("igsn_lib.models")

Base = sqlalchemy.ext.declarative.declarative_base()

def dtToJson(dt):
    if dt is None:
        return None
    return dt.strftime(igsn_lib.time.JSON_TIME_FORMAT)

class IGSN(Base):
    """
    Defines a minimal IGSN record to capture information provided from an OAI-PMH endpoint.
    """

    __tablename__ = "igsn"
    id = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        doc="id is the IGSN value, must be unique in the datastore",
    )
    oai_id = sqlalchemy.Column(sqlalchemy.String, doc="The OAI-PMH record id.")
    service_id = sqlalchemy.Column(
        sqlalchemy.Integer, sqlalchemy.ForeignKey("service.id")
    )
    record = sqlalchemy.orm.relationship("Service", back_populates="records")
    harvest_time = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        default=igsn_lib.time.dtnow,
        doc="When the record was harvested, UTC datetime",
    )
    oai_time = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        doc="Timestamp reported for OAI-PMH record, UTC datetime",
    )
    igsn_time = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        doc="time reported in the IGSN record submitted or registered log event,UTC datetime",
    )
    registrant = sqlalchemy.Column(
        sqlalchemy.String,
        nullable=True,
        doc="registrant name reported in the IGSN record",
    )
    related = sqlalchemy.Column(
        sqlalchemy.JSON,
        nullable=True,
        default=None,
        doc="related identifiers in the IGSN record",
    )
    log = sqlalchemy.Column(
        sqlalchemy.JSON, nullable=True, default=None, doc="log entries in IGSN record"
    )

    def __repr__(self):
        return json.dumps(self.asJsonDict(), indent=2)

    def asJsonDict(self):
        """
        Provide a JSON serializable dict representation of instance.

        Returns:
            dict
        """
        d = {
            "id": self.id,
            "oai_id": self.oai_id,
            "service_id": self.service_id,
            "harvest_time": dtToJson(self.harvest_time),
            "oai_time": dtToJson(self.oai_time),
            "igsn_time": dtToJson(self.igsn_time),
            "registrant": self.registrant,
            "related": self.related,
            "log": self.log,
        }
        return d

    def fromOAIRecord(self, record):
        '''
        Populates self from an OAI-PMH IGSN XML string

        Args:
            record: String, xml OAI-PMH record

        Returns:
            nothing

        Examples:

            .. jupyter-execute::

               import igsn_lib.models
               xml = """<?xml version="1.0"?>
                <record xmlns="http://www.openarchives.org/OAI/2.0/"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  <header>
                    <identifier>oai:registry.igsn.org:6940929</identifier>
                    <datestamp>2019-10-15T06:00:10Z</datestamp>
                    <setSpec>IEDA</setSpec>
                    <setSpec>IEDA.SESAR</setSpec>
                  </header>
                  <metadata>
                    <sample xmlns="http://igsn.org/schema/kernel-v.1.0"
                            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                            xsi:schemaLocation="http://igsn.org/schema/kernel-v.1.0 http://doidb.wdc-terra.org/igsn/schemas/igsn.org/schema/1.0/igsn.xsd">
                      <sampleNumber identifierType="igsn">10273/BSU0005JF</sampleNumber>
                      <registrant>
                        <registrantName>IEDA</registrantName>
                      </registrant>
                      <log>
                        <logElement event="submitted" timeStamp="2019-10-15T04:00:09Z"/>
                      </log>
                    </sample>
                  </metadata>
                </record>
               """
               record = igsn_lib.models.IGSN()
               record.fromOAIRecord(xml)
               print(record)

        '''
        data = igsn_lib.oai.oaiRecordToDict(record)
        self.id = data["igsn_id"]
        self.oai_id = data["oai_id"]
        self.oai_time = data["oai_time"]
        self.registrant = data["registrant"]
        self.igsn_time = data["igsn_time"]
        if len(data["related"]) > 0:
            self.related = data["related"]
        if len(data["log"]) > 0:
            self.log = data["log"]


class Job(Base):
    """
    Describes an OAI-PMH  harvest job.
    """

    __tablename__ = "job"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    service_id = sqlalchemy.Column(
        sqlalchemy.Integer, sqlalchemy.ForeignKey("service.id")
    )
    service = sqlalchemy.orm.relationship("Service", back_populates="jobs")
    tstart = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Time when the job was started.",
    )
    tend = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Time when the job was completed.",
    )
    ignore_deleted = sqlalchemy.Column(
        sqlalchemy.Boolean,
        default=True,
        doc="Ignore records flagged by the OAI-PMH provider as deleted.",
    )
    metadata_prefix = sqlalchemy.Column(
        sqlalchemy.String,
        default="igsn",
        doc="The metadata prefix to use when requesting records.",
    )
    setspec = sqlalchemy.Column(
        sqlalchemy.String,
        nullable=True,
        doc="The OAI-PMH set to use when requesting records.",
    )
    tfrom = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        doc="start of time range for this job, OAI time range boundaries are inclusive",
    )
    tuntil = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        doc="end of time range for this job, OAI time range boundaries are inclusive",
    )
    tlast_record = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        doc="Time stamp of last retrieved, comitted record. Resumption starts here.",
    )

    def asDict(self):
        d = {
            "id": self.id,
            "service_id": self.service_id,
            "tstart": dtToJson(self.tstart),
            "tend": dtToJson(self.tend),
            "ignore_deleted": self.ignore_deleted,
            "metadata_prefix": self.metadata_prefix,
            "setspec": self.setspec,
            "tfrom": dtToJson(self.tfrom),
            "tuntil": dtToJson(self.tuntil),
            "tlast_record": dtToJson(self.tlast_record),
        }
        return d

    def __repr__(self):
        return json.dumps(self.asDict(), indent=2)

    def execute(self, session, callback=None, resume=True):
        """
        Execute this task, harvesting records until complete.

        #TODO: implement support for resumption of interupted job

        Args:
            session: sqlalchemy session
            callback: optional callback mutate record before committing
            resume: If True, start retrieval from the last retrieved date

        Returns:
            integer, number of records added
        """
        svc = igsn_lib.oai.getSickle(self.service.url)
        kwargs = {"metadataPrefix": self.metadata_prefix}
        if self.setspec is not None:
            kwargs["set"] = self.setspec
        if self.tfrom is not None:
            kwargs["from"] = self.tfrom.strftime(igsn_lib.time.OAI_TIME_FORMAT)
        if self.tuntil is not None:
            kwargs["until"] = self.tuntil.strftime(igsn_lib.time.OAI_TIME_FORMAT)
        if resume and self.tlast_record is not None:
            if self.tuntil is not None:
                if self.tlast_record > self.tuntil:
                    _L.error(
                        "Start date (%s) must be less than end date (%s)",
                        self.tlast_record,
                        self.tuntil,
                    )
                    return 0
            kwargs["from"] = self.tlast_record.strftime(igsn_lib.time.OAI_TIME_FORMAT)
            _L.info("Resuming harvest job from %s", kwargs["from"])

        self.tstart = igsn_lib.time.dtnow()
        self.tend = None
        counter = 0
        new_count = 0
        total_count = -1
        try:
            records = svc.ListRecords(ignore_deleted=self.ignore_deleted, **kwargs)
        except sickle.oaiexceptions.NoRecordsMatch as e:
            _L.warning("No records found for job.id = %s", self.id)
            self.tend = igsn_lib.time.dtnow()
            session.commit()
            return counter
        for record in records:
            if total_count < 0:
                _L.info(
                    "OAI-PMH batch has %s records",
                    records.resumption_token.complete_list_size,
                )
            total_count = int(records.resumption_token.complete_list_size)
            try:
                igsn = IGSN(
                    service_id=self.service_id, harvest_time=igsn_lib.time.dtnow()
                )
                igsn.fromOAIRecord(record.raw)
                _L.debug(igsn)
                exists = session.query(IGSN).get(igsn.id)
                if not exists:
                    _L.debug("NEW")
                    if callback is not None:
                        callback(record, igsn)
                    new_count += 1
                    try:
                        session.add(igsn)
                        self.tlast_record = igsn.oai_time
                        session.commit()
                    except sqlalchemy.exc.IntegrityError as e:
                        _L.warning("IGSN entry already exists: %s", str(igsn))
                else:
                    _L.debug("EXISTING")
            except Exception as e:
                _L.error(e)
            counter += 1
            if counter % 10 == 0:
                _L.info("%s (%s) / %s", new_count, counter, total_count)
        self.tend = igsn_lib.time.dtnow()
        session.commit()
        return new_count, counter, total_count


class Service(Base):
    __tablename__ = "service"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    url = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    tearliest = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
    name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=True)
    admin_email = sqlalchemy.Column(sqlalchemy.String, nullable=True)

    def __repr__(self):
        d = self.asJsonDict()
        return json.dumps(d, indent=2)

    def asJsonDict(self):
        d = {
            "id": self.id,
            "url": self.url,
            "tearliest": dtToJson(self.tearliest),
            "name": self.name,
            "admin_email": self.admin_email,
        }
        return d

    def populate(self, session=None, url=None):
        """
        Populate record from Identify service

        If session is provided then record is commited on success

        Args:
            session: database session
            url: option url to use for base_url

        Returns: nothing
        """
        if not url is None:
            self.url = url
        info = igsn_lib.oai.identify(self.url)
        self.name = info.repositoryName
        self.tearliest = dateparser.parse(
            info.earliestDatestamp,
            settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE": True},
        )
        self.admin_email = info.adminEmail
        if session is not None:
            session.commit()

    def listSets(self, get_counts=False):
        oai_svc = igsn_lib.oai.getSickle(self.url)
        return igsn_lib.oai.listSets(oai_svc, get_counts=get_counts)

    def mostRecentRecordRetrieved(self, session):
        """
        Get the most recent harvested record for this service

        Returns: IGSN
        """
        rec = (
            session.query(IGSN)
            .join(Service)
            .filter(Service.id == self.id)
            .order_by(IGSN.oai_time.desc())
            .first()
        )
        return rec

    def topupHarvestJob(
        self, session, ignore_deleted=True, metadata_prefix="igsn", setspec=None
    ):
        last_rec = self.mostRecentRecordRetrieved(session)
        logging.debug("RECENT RECORD: %s", last_rec)
        job = self.createJob(
            session=session,
            ignore_deleted=ignore_deleted,
            metadata_prefix=metadata_prefix,
            setspec=setspec,
            tfrom=last_rec.oai_time,
        )
        self.jobs.append(job)
        session.commit()
        return job

    def createJob(
        self,
        session=None,
        ignore_deleted=True,
        metadata_prefix="igsn",
        setspec=None,
        tfrom=None,
        tuntil=None,
    ):
        job = Job(
            service_id=self.id,
            ignore_deleted=ignore_deleted,
            metadata_prefix=metadata_prefix,
            setspec=setspec,
            tfrom=tfrom,
            tuntil=tuntil,
        )
        self.jobs.append(job)
        if session is not None:
            session.commit()
        return job

    def createJobPackage(
        self,
        session=None,
        ignore_deleted=True,
        metadata_prefix="igsn",
        setspec=None,
        tfrom=None,
        tuntil=None,
        tdelta=50,
    ):
        """
        Generates a set of jobs to do a bunch of harvesting

        Args:
            session: database session
            tfrom: Starting date, defaults to tearliest
            tuntil: Ending date, defaults to now
            tdelta: maximum period in days for each job, defaults to 50 days

        Returns:
            list of Jobs
        """
        if tfrom is None:
            tfrom = self.tearliest
        if tuntil is None:
            tuntil = igsn_lib.time.dtnow()
        tfrom_jd = igsn_lib.time.datetimeToJD(tfrom)
        tuntil_jd = igsn_lib.time.datetimeToJD(tuntil)
        _delta = tuntil_jd - tfrom_jd
        jobs = []
        if _delta <= tdelta:
            jobs.append(
                self.createJob(
                    session=session,
                    ignore_deleted=ignore_deleted,
                    metadata_prefix=metadata_prefix,
                    setspec=setspec,
                    tfrom=tfrom,
                    tuntil=tuntil,
                )
            )
            return jobs
        t2_jd = tfrom_jd + tdelta
        while t2_jd < tuntil_jd:
            jobs.append(
                self.createJob(
                    session=session,
                    ignore_deleted=ignore_deleted,
                    metadata_prefix=metadata_prefix,
                    setspec=setspec,
                    tfrom=igsn_lib.time.jdToDateTime(tfrom_jd),
                    tuntil=igsn_lib.time.jdToDateTime(t2_jd),
                )
            )
            tfrom_jd = t2_jd
            t2_jd = tfrom_jd + tdelta
        return jobs


Service.jobs = sqlalchemy.orm.relationship(
    "Job", order_by=Job.id, back_populates="service"
)

Service.records = sqlalchemy.orm.relationship(
    "IGSN", order_by=IGSN.id, back_populates="record"
)


def createAll(engine):
    """
    Create the database tables etc if not aleady present.

    Args:
        engine: SqlAlchemy engine to use.

    Returns:
        nothing
    """
    Base.metadata.create_all(engine)


def getOrCreate(session, model, create_method="", create_method_kwargs=None, **kwargs):
    """
    Get or create and get a record.

    Find a record that matches a query on kwargs. Create the record if
    nothing matches. Return the found or created record.

    Args:
        session: sqlalchemy session
        model: the model (table) to work with
        create_method: The method to use for creating the record
        create_method_kwargs: kwargs to pass to the create method
        **kwargs: kwargs to query for an existing record

    Returns:
        tuple, (record, True if created)
    """
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except sqlalchemy.orm.exc.NoResultFound:
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
    """
    Add and OAI-PMH service record.

    The identify information from the endpoint is used to populate the record.

    Args:
        session: An sqlalchemy session
        url: OAI-PMH service endpoint.

    Returns:
        Service instance
    """
    svc, _ = getOrCreate(session, Service, url=url)
    if svc.tearliest is None:
        svc.populate(session=session)
    return svc
