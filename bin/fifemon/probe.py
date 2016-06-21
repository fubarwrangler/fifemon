#!/usr/bin/python
import logging
import time

from graphite import Graphite

logger = logging.getLogger(__name__)


class Probe(object):
    def __init__(self, *args, **kwargs):
        self.retries = kwargs.pop('retries', 3)
        self.delay = kwargs.pop('delay', 20)
        self.test = kwargs.pop('test', True)
        self.once = self.test or kwargs.pop('once', False)

        self.graphite_host = kwargs.pop('graphite', 'localhost')
        self.graphite_pickle_port = kwargs.pop('pickle_port', 2004)
        self.namespace = kwargs.pop('namespace', 'test')

        if self.test:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        self.graphite = Graphite(self.graphite_host, self.graphite_pickle_port)

    def __unicode__(self):
        return """
namespace:      %s
interval:       %d s
retries:        %d
delay:          %d s
test:           %s
once:           %s

graphite_host:  %s
graphite_pickle_port:  %s

""" % (self.namespace,
            self.meta_namespace,
            self.interval,
            self.retries,
            self.delay,
            self.test,
            self.once,
            self.graphite_host,
            self.graphite_pickle_port)

    def __str__(self):
        return self.__unicode__()

    def post(self):
        pass

    def run(self):
        start = time.time()
        self.post()
        duration = time.time()-start
        logger.info("({0}) posted data in {1} s".format(self.namespace, duration))
        meta_data = {
            "update_time": duration,
        }
        self.graphite.send_dict(self.pool, meta_data, send_data=(not self.test))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    p = Probe(test=True)
    logger.debug(p)
    p.run()
