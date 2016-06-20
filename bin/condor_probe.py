#!/usr/bin/env python
from optparse import OptionParser
import logging
import os
import ConfigParser
import pprint

import fifemon
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
        self.pool = kwargs.pop('pool', 'localhost')
        self.post_pool_status = kwargs.pop('post_pool_status', True)
        self.post_pool_slots = kwargs.pop('post_pool_slots', True)
        self.post_pool_glideins = kwargs.pop('post_pool_glideins', False)
        self.post_pool_prio = kwargs.pop('post_pool_prio', True)
        self.post_pool_jobs = kwargs.pop('post_pool_jobs', False)
        self.use_gsi_auth = kwargs.pop('use_gsi_auth', False)
        self.x509_user_key = kwargs.pop('x509_user_key', "")
        self.x509_user_cert = kwargs.pop('x509_user_cert', "")

        if self.post_pool_jobs:
            self.jobs = condor.Jobs(self.pool)

        super(CondorProbe, self).__init__(*args, **kwargs)

    def post(self):
        if self.use_gsi_auth:
            save_key = os.environ.get('X509_USER_KEY')
            os.environ['X509_USER_KEY'] = self.x509_user_key
            save_cert = os.environ.get('X509_USER_CERT')
            os.environ['X509_USER_CERT'] = self.x509_user_cert

        if self.post_pool_status:
            logger.info('querying pool {0} status'.format(self.pool))
            data = condor.get_pool_status(self.pool, self.delay, self.retries)
            for dataset in data:
                if self.use_graphite:
                    self.graphite.send_dict(self.namespace,
                                            dataset["metrics"],
                                            send_data=(not self.test))
        if self.post_pool_slots:
            logger.info('querying pool {0} slots'.format(self.pool))
            data = condor.get_pool_slots(self.pool, self.delay, self.retries)
            if self.use_graphite:
                self.graphite.send_dict(self.namespace+".slots", data, send_data=(not self.test))
        if self.post_pool_prio:
            logger.info('querying pool {0} priorities'.format(self.pool))
            data = condor.get_pool_priorities(self.pool, self.delay, self.retries)
            if self.use_graphite:
                self.graphite.send_dict(self.namespace+".priorities", data, send_data=(not self.test))
        if self.post_pool_jobs:
            logger.info('querying pool {0} jobs'.format(self.pool))
            data = self.jobs.get_job_count(self.delay, self.retries)
            if self.use_graphite:
                self.graphite.send_dict(self.namespace+".jobs", data, send_data=(not self.test))

        if self.use_gsi_auth:
            if save_key is None:
                del os.environ['X509_USER_KEY']
            else:
                os.environ['X509_USER_KEY'] = save_key
            if save_cert is None:
                del os.environ['X509_USER_CERT']
            else:
                os.environ['X509_USER_CERT'] = save_cert


def get_options():
    parser = OptionParser(usage="usage: %prog [options] [config file(s)]")
    parser.add_option('-t', '--test', action="store_true",
                      help="output data to stdout, don't send to graphite (implies --once)")
    parser.add_option('-1', '--once', action="store_true",
                      help="run once and exit")
    (cmd_opts, args) = parser.parse_args()

    config = ConfigParser.SafeConfigParser()
    # config.readfp(file("etc/condor/defaults.cfg"))
    config.read(args)

    def parse_tags(tags):
        if tags is None or tags == "":
            return None
        r = {}
        for k, v in [kv.split(":") for kv in tags.split(",")]:
            r[k] = v
        return r

    opts = {
        'pool':              config.get("condor", "pool"),
        'post_pool_status':  config.getboolean("condor", "post_pool_status"),
        'post_pool_slots':   config.getboolean("condor", "post_pool_slots"),
        'post_pool_glideins': config.getboolean("condor", "post_pool_glideins"),
        'post_pool_prio':    config.getboolean("condor", "post_pool_prio"),
        'post_pool_jobs':    config.getboolean("condor", "post_pool_jobs"),
        'use_gsi_auth':      config.getboolean("condor", "use_gsi_auth"),
        'x509_user_key':     config.get("condor", "X509_USER_KEY"),
        'x509_user_cert':    config.get("condor", "X509_USER_CERT"),
        'use_graphite':      config.getboolean("graphite", "enable"),
        'namespace':         config.get("graphite", "namespace"),
        'meta_namespace':    config.get("graphite", "meta_namespace"),
        'graphite_host':     config.get("graphite", "host"),
        'graphite_port':     config.getint("graphite", "port"),
        'test':              cmd_opts.test or config.getboolean("probe", "test"),
        'once':              cmd_opts.once or config.getboolean("probe", "once"),
        'interval':          config.getint("probe", "interval"),
        'delay':             config.getint("probe", "delay"),
        'retries':           config.getint("probe", "retries"),
    }

    return opts

if __name__ == '__main__':
    opts = get_options()
    if opts['test']:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logging.basicConfig(level=loglevel, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    logger.info('Probe configuraion: \n'+pprint.pformat(opts))

    probe = CondorProbe(**opts)
    probe.run()
