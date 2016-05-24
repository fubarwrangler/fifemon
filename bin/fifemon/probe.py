#!/usr/bin/python
import logging
import time

from graphite import Graphite
from influx import Influxdb

logger = logging.getLogger(__name__)

class Probe(object):
    def __init__(self, *args, **kwargs):
        self.interval = kwargs.pop('interval', 240)
        self.retries = kwargs.pop('retries', 10)
        self.delay = kwargs.pop('delay', 30)
        self.test = kwargs.pop('test', True)
        self.once = self.test or kwargs.pop('once', False)

        self.use_graphite = kwargs.pop('use_graphite', True)
        self.graphite_host = kwargs.pop('graphite_host', 'localhost')
        self.graphite_pickle_port = kwargs.pop('graphite_pickle_port', 2004)
        self.namespace = kwargs.pop('namespace', 'test')
        self.meta_namespace = kwargs.pop('meta_namespace', 'probes.test')

        self.use_influxdb = kwargs.pop('use_influxdb', False)
        self.influxdb_host = kwargs.pop('influxdb_host', 'localhost')
        self.influxdb_port = kwargs.pop('influxdb_port', 8086)
        self.influxdb_db = kwargs.pop('influxdb_db', 'test')
        self.influxdb_tags = kwargs.pop('influxdb_tags', {})

        if self.test:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if self.use_graphite:
            self.graphite = Graphite(self.graphite_host, self.graphite_pickle_port)
        if self.use_influxdb:
            self.influxdb = Influxdb(self.influxdb_host, self.influxdb_port, self.influxdb_db)

    def __unicode__(self):
        return """
namespace:      %s
meta_namespace: %s
interval:       %d s
retries:        %d
delay:          %d s
test:           %s
once:           %s

use_graphite:   %s
graphite_host:  %s
graphite_pickle_port:  %s

use_influxdb:   %s
influxdb_host:  %s
influxdb_port:  %d
influxdb_db:    %d

""" % \
(self.namespace,
    self.meta_namespace,
    self.interval,
    self.retries,
    self.delay,
    self.test,
    self.once,
    self.use_graphite,
    self.graphite_host,
    self.graphite_pickle_port,
    self.use_influxdb,
    self.influxdb_host,
    self.influxdb_port,
    self.influxdb_db)

    def __str__(self):
        return self.__unicode__()

    def post(self):
        pass

    def run(self):
        while True:
            start = time.time()
            self.post()
            duration = time.time()-start
            logger.info("({0}) posted data in {1} s".format(self.namespace, duration))
            meta_data = {
                "update_time": duration,
            }
            if self.use_graphite:
                self.graphite.send_dict(self.meta_namespace, meta_data, send_data=(not self.test))
            if self.use_influxdb:
                pass
            sleep = max(self.interval-duration-10, 0)
            logger.info("({0}) sleeping {1} s".format(self.namespace, sleep))
            if self.test or self.once:
                return
            time.sleep(sleep)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    p = Probe(test=True)
    logger.debug(p)
    p.run()
