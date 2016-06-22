#!/usr/bin/env python
from optparse import OptionParser
import logging
import ConfigParser
import sys

import fifemon
import htcondor
import condor

logger = logging.getLogger(__name__)


class CondorProbe(fifemon.Probe):
    """
    Query HTCondor pool and post statistics to Graphite.

    Options:
        post_pool_status:   collect main daemon (schedd, collector,
                            negotiator) statistics
        post_pool_slots:    collect & aggregate slot (startd) status
        post_pool_prio:     collect user priorities
        post_pool_jobs:     collect & aggregate user job status
    """

    def __init__(self, *args, **kwargs):
        self.meta = args[0]
        self.address = self.meta['address']
        self.post_pool_status = kwargs.pop('post_pool_status', True)
        self.post_pool_slots = kwargs.pop('post_pool_slots', True)
        self.post_pool_prio = kwargs.pop('post_pool_prio', True)
        self.post_pool_jobs = kwargs.pop('post_pool_jobs', False)

        self.pool = htcondor.Collector(self.address)

        super(CondorProbe, self).__init__(*args, **kwargs)

    def post(self):

        # logger.info('querying pool {0} status'.format(self.pool))
        # data = condor.get_pool_status(self.pool, self.delay, self.retries)
        # print len(data)
        # for dataset in data:
        #     self.graphite.send_dict(self.namespace,
        #                             dataset["metrics"],
        #                             send_data=(not self.test))
        logger.info('querying pool %s slots', self.pool)
        data = condor.get_pool_slots(self.pool, self.delay, self.retries)
        self.graphite.send_dict(self.namespace+".slots", data, send_data=(not self.test))
        # if self.post_pool_prio:
        #     logger.info('querying pool {0} priorities'.format(self.pool))
        #     data = condor.get_pool_priorities(self.pool, self.delay, self.retries)
        #     if self.use_graphite:
        #         self.graphite.send_dict(self.namespace+".priorities", data, send_data=(not self.test))
        # if self.post_pool_jobs:
        #     logger.info('querying pool {0} jobs'.format(self.pool))
        #     data = self.jobs.get_job_count(self.delay, self.retries)
        #     if self.use_graphite:
        #         self.graphite.send_dict(self.namespace+".jobs", data, send_data=(not self.test))


def get_options():
    parser = OptionParser(usage="usage: %prog [options] [config-file] pool")
    parser.add_option('-t', '--test', action="store_true", default=False,
                      help="output data to stdout, don't send (implies debug)")
    parser.add_option('-d', '--debug', action="store_true", default=False,
                      help="enable debug-logging")

    (cmd_opts, args) = parser.parse_args()

    cfg = ConfigParser.SafeConfigParser()
    cfg.read(args[0])
    if len(args) < 1:
        parser.error("Need a pool-name argument")
        sys.exit(1)

    name = args[1]

    pool = {}
    pool['pool'] = name
    for k, v in cfg.items(name):
        if k in ('attrs', 'groups'):
            v = [x.strip() for x in v.split(',')]
        pool[k] = v

    opts = {
        'debug': cmd_opts.debug or cmd_opts.test,
        'test': cmd_opts.test,
        'graphite': cfg.get('graphite', 'graphite_host'),
        'port': cfg.getint('graphite', 'pickle_port'),
        'namespace': cfg.get('graphite', 'namespace') + '.' + name,
    }

    return opts, pool

if __name__ == '__main__':
    opts, pool = get_options()
    loglevel = logging.DEBUG if opts['debug'] else logging.INFO
    logging.basicConfig(level=loglevel, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    print opts, pool
    probe = CondorProbe(pool, **opts)
    print probe.pool
    probe.run()
    # probe = CondorProbe(**opts)
    # probe.run()
