# WARNING! Legacy module!

import os
import sys
import json
import dateparser
from datetime import datetime
from decimal import Decimal

from cassandra.cluster import Cluster

import settings.base as conf


class CassandraProxy(object):
    fields_extract = [
        'tweetid', 'created_at', 'ttype', 'annotations',
        'geotags', 'lang', 'latlong', 'mordecai_raw', 'tweet'
        ]

    def __init__(self, *args, **kwargs):
        self.nodes = list(args)
        if self.nodes == []:
            self.nodes = [conf.CASSANDRA_NODE_ADDRESS]
        self.keyspace = kwargs.get('keyspace', conf.CASSANDRA_KEYSPACE)

    def open_connection(self):
        self.cluster = Cluster(self.nodes)
        self.session = self.cluster.connect(self.keyspace)
        name = self.__class__.__name__
        print("~ [{}] connect to cluster: {}".format(name, self.cluster))
        print("~ [{}] open session: {}".format(name, self.session))

    def cleanup(self):
        print("~ [{}] session shutdown".format(self.__class__.__name__))
        self.session.shutdown()
        self.cluster.shutdown()

    def _prepare_record(self, obj):
        """
        Convert all data to dict.

        WARNING! Not annotated records or records without flood_probability
        and / or latlong are being discarded!
        """
        try:
            tweet = json.loads(obj.tweet)
        except (TypeError, ValueError):
            return None

        if (obj.annotations is None) or (obj.latlong is None):
            return None

        data = {}
        for field in self.fields_extract:
            data.update({field: getattr(obj, field)})
        data.update({'tweet': tweet}) # Json instead of string
        return data

    def get_data(self, timestamp, **kwargs):
        """
        Iterator through the results of cursor containing data from Cassandra.
        Prepares data to pass over to celery task (must be dict!).
        Ignores objects with no flood prob or those that aren't geotagged.

        :param table: str - table name
        :param timestamp: datetime.datetime or string

        :kwargs timeout: seconds
        :kwargs table: str (conf.CASSANDRA_DEFAULT_TABLE if not provided)
        :kwargs timestamp_to: datetime.datetime or string
        :kwargs limit: int

        :return: dict
        """
        table = kwargs.get('table', conf.CASSANDRA_DEFAULT_TABLE)
        timeout = kwargs.get('timeout', 30)
        qry = self.build_query(timestamp, table, **kwargs)

        self.open_connection()
        for obj in self.execute_query(qry, timeout=timeout):
            data = self._prepare_record(obj)
            if data:
                yield data

        self.cleanup()

    def execute_query(self, qry, timeout):
        return self.session.execute(qry, timeout=timeout)

    def build_query(self, timestamp, table, **kwargs):
        """
        :param table: str - table name
        :param timestamp: datetime.datetime
        :kwargs limit: int
        """
        if isinstance(timestamp, str):
            timestamp = dateparser.parse(timestamp)

        qry = "SELECT {}\n".format(", ".join(self.fields_extract))
        qry += "FROM {}\n".format(table)
        qry += "WHERE monthbucket = '{}'\n".format(timestamp.strftime('%Y-%m'))
        qry += "AND ttype = 'geoparsed'\n"
        qry += "AND collectionid = {}\n".format(conf.CASSANDRA_COLLECTION_ID)
        qry += "AND created_at >= '{}'".format(
            timestamp.strftime('%Y-%m-%d %H:%M:%S'))

        # Optional filters.
        timestamp_to = kwargs.get('timestamp_to', None)
        if timestamp_to:
            if isinstance(timestamp_to, str):
                timestamp_to = dateparser.parse(timestamp_to)
            qry += "\nAND created_at < '{}'".format(
                timestamp_to.strftime('%Y-%m-%d %H:%M:%S'))
        limit = kwargs.get('limit', None)
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                raise
            else:
                qry += "\nLIMIT {}".format(limit)

        qry += ";"
        print("~ [{}] query:\n{}".format(self.__class__.__name__, qry))
        return qry
