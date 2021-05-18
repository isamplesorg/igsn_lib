'''
Implements sqlalchemy model of relationship between things.

A Relation is modelled as an RDF triple (subject s, predicate p, object o).

A Relation may be part of a named graph, in which case the `name` property is the
identifier of the graph.

A Relation is defined within some `source`. In many cases the `source` will be the
same as the `subject`.

The combination of source, name, s, p, o is unique.
'''

import logging
import json
import sqlalchemy.dialects.postgresql
import igsn_lib.time
import igsn_lib.models

class Relation(igsn_lib.models.Base):

    __tablename__ = "relation"
    tstamp = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        default=igsn_lib.time.dtnow(),
        doc="When the entry was added to this database",
    )
    source = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        nullable=True,
        index = True,
        doc="Identifier of the source of this relation"
    )
    name = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        nullable=True,
        index = True,
        doc="Optional name if relation is part of named graph"
    )
    s = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        nullable=False,
        index = True,
        doc="subject identifier of the relation"
    )
    p = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        nullable=False,
        index = True,
        doc="predicate identifier of the relation"
    )
    o = sqlalchemy.Column(
        sqlalchemy.String,
        primary_key=True,
        nullable=False,
        index = True,
        doc="object identifier of the relation"
    )

    def __repr__(self):
        return json.dumps(self.asJsonDict(), indent=2)

    def asJsonDict(self):
        res = {
            'tstamp': igsn_lib.time.datetimeToJsonStr(self.tstamp),
            'source': self.source,
            'name': self.name,
            's': self.s,
            'p': self.p,
            'o': self.o,
        }
        return res

