import logging
import json
import dateparser
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib.time
import igsn_lib.models

class Thing(igsn_lib.models.Base):

    __tablename__ = "thing"
    id = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        doc="identifier scheme:value, globally unique"
    )
    tstamp = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        default=igsn_lib.time.dtnow,
        doc="When the entry was added to this database, UTC",
    )
    tcreated = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        default = None,
        nullable=True,
        doc = "When the record was created, if available",
    )
    item_type = sqlalchemy.Column(
        sqlalchemy.String,
        default=None,
        nullable=True,
        doc = "Type of thing described by this identifier"
    )
    authority_id = sqlalchemy.Column(
        sqlalchemy.String,
        nullable=True,
        default=None,
        doc="Authority of this thing",
    )
    related = sqlalchemy.Column(
        sqlalchemy.JSON,
        nullable=True,
        default=None,
        doc="related things [{tstamp, predicate, object}]",
    )
    log = sqlalchemy.Column(
        sqlalchemy.JSON, nullable=True, default=None, doc="log entries in IGSN record"
    )
    resolved_url = sqlalchemy.Column(
        sqlalchemy.String,
        default=None,
        nullable=True,
        doc = "URL that was resolved for the identifier"
    )
    resolved_status = sqlalchemy.Column(
        sqlalchemy.Integer,
        default=None,
        nullable=True,
        doc = "Status code of the resolve response"
    )
    tresolved = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        default = None,
        nullable=True,
        doc = "When the record was resolved",
    )
    resolved_content = sqlalchemy.Column(
        sqlalchemy.JSON,
        nullable=True,
        default=None,
        doc="Resolved content, {content_type:, content: }",
    )


    def __repr__(self):
        return json.dumps(self.asJsonDict(), indent=2)

    def asJsonDict(self):
        res = {
            'id':self.id,
            'tstamp': igsn_lib.models.dtToJson(self.tstamp),
            'tcreated':igsn_lib.models.dtToJson(self.tcreated),
            'item_type': self.item_type,
            'authority_id': self.authority_id,
            'related': self.related,
            'log': self.log,
            'resolved_url': self.resolved_url,
            'resolved_status': self.resolved_status,
            'tresolved': igsn_lib.models.dtToJson(self.tresolved),
            'resolved_content': self.resolved_content,
        }
        return res

    def resolve(self, session):

        pass


def resolveThing(session, identifier):
    pass