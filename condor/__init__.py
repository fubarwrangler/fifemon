import os
os.environ['_CONDOR_GSI_SKIP_HOST_CHECK'] = "true"


import probes

import logging
import htcondor

logger = logging.getLogger(__name__)


class CondorProbe(probes.Probe):
    """
    Query HTCondor pool and post statistics to Graphite.

    Options:
        post_pool_status:   collect main daemon (schedd, collector,
                            negotiator) statistics
        post_pool_slots:    collect & aggregate slot (startd) status
        post_pool_prio:     collect user priorities
        post_pool_jobs:     collect & aggregate user job status
    """

    all_probes = set(['slots', 'status', 'prio', 'jobs'])

    from .status import get_pool_status             # noqa: not used
    from .slots import get_pool_slots               # noqa: not used
    from .priorities import get_pool_priorities     # noqa: not used

    def __init__(self, *args, **kwargs):
        self.meta = args[0]
        self.address = self.meta['address']

        self.name = '{0}@{1}'.format(self.meta['pool'], self.address)

        self.post_pool_status = kwargs.pop('post_pool_status', True)
        self.post_pool_slots = kwargs.pop('post_pool_slots', True)
        self.post_pool_prio = kwargs.pop('post_pool_prio', True)
        self.post_pool_jobs = kwargs.pop('post_pool_jobs', False)

        self.retries = kwargs.pop('retries', 1)
        self.delay = kwargs.pop('delay', 10)

        self.probes = set(kwargs.pop('probes', []))

        if not self.probes:
            self.probes = self.all_probes

        if not self.probes.issubset(self.all_probes):
            bad = self.probes - self.all_probes
            logger.error("%d unknown probes found: %s", len(bad), ', '.join(sorted(bad)))
            raise Exception("Bad probes")

        self.pool = htcondor.Collector(self.address)

        super(CondorProbe, self).__init__(*args, **kwargs)

    def local_schedds(self):
        for ad in self.get_daemons('schedd'):
            if ad.get('CollectorHost') == self.address:
                yield ad

    def get_daemons(self, daemon_type):
        dtype = {'schedd': htcondor.DaemonTypes.Schedd,
                 'collector': htcondor.DaemonTypes.Collector,
                 'negotiator': htcondor.DaemonTypes.Negotiator
                 }[daemon_type]
        logger.debug("Search %s for daemon type %s", self.name, daemon_type)
        return self.pool.locateAll(dtype)

    def startd_query(self, projection=[], constraint=True):

        try:
            ads = self.pool.query(htcondor.AdTypes.Startd, constraint, projection)
        except:
            logger.exception("Failed to query collector (%s) for startd ads", self.name)
            ads = None
        return ads

    def post(self):
        logger.debug("Probes to run: %s", ', '.join(self.probes))
        success = 0

        if 'status' in self.probes:
            logger.info('querying pool %s status', self.name)
            data = self.get_pool_status()
            if data:
                self.graphite.send_dict(self.namespace, data)
                success += 1

        if 'slots' in self.probes:
            logger.info('querying pool %s slots', self.name)
            data = self.get_pool_slots(extras=self.meta.get('attrs', []))
            if data:
                self.graphite.send_dict(self.namespace+".slots", data)
                success += 1

        if 'prio' in self.probes:
            logger.info('querying pool %s priorities', self.name)
            data = self.get_pool_priorities()
            if data:
                self.graphite.send_dict(self.namespace+".priorities", data)
                success += 1

        return success > 0

        # if self.post_pool_jobs:
        #     logger.info('querying pool %s jobs', self.name)
        #     data = self.jobs.get_job_count()
        #     self.graphite.send_dict(self.namespace+".jobs", data, send_data=(not self.test))
