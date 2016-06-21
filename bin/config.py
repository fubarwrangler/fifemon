import logging
import htcondor

logger = logging.getLogger(__name__)

base_fields = set(['State', 'Activity', 'Owner', 'Name', 'SlotType', 'DaemonStartTime',
                   'MyCurrentTime', 'Disk', 'Memory', 'Cpus', 'LoadAvg', 'TotalCondorLoadAvg',
                   'Turn_Off', 'RemoteOwner', 'PreemptingOwner', 'RealExperiment'])


class Pool(object):

    def __init__(self, name, address, groups, fields=set()):
        self.pool = address
        self.fields = base_fields | fields
        self.groups = set([groups]) if type(groups) == type(str) else set(groups)
        self.collector = htcondor.Collector(self.pool)

    def local_schedds(self):
        for ad in self.get_daemon('schedd'):
            if ad.get('CollectorHost') == self.pool:
                yield ad

    def get_daemon(self, daemon):
        dtype = {'schedd': htcondor.DaemonTypes.Schedd,
                 'collecor': htcondor.DaemonTypes.Collector,
                 'negotiator': htcondor.DaemonTypes.Negotiator
                 }[daemon]
        logger.debug("Search %s for daemon type %s", self.pool, daemon)
        return self.collector.locateAll(dtype)

pools = {
    'atlas':
        Pool('atlas', 'condor03.usatlas.bnl.gov:9660', 'usatlas',
             set(['CPU_Type', 'RACF_Group', 'AccountingGroup', 'SlotWeight'])),

    'localt3':
        Pool('localt3', 'condor04.usatlas.bnl.gov:9667', 'usatlas',
             set(['CPU_Type', 'RACF_Group', 'AccountingGroup', 'Job_Type', 'AcctGroup'])),

    'brahms':
        Pool('brahms', 'condor02.rcf.bnl.gov:9661', ['rhbrahms', 'dayabay', 'lbne', 'eic', 'astro'],
             set(['CPU_Type', 'Job_Type', 'CPU_Experiment'])),
    'phenix':
        Pool('phenix', 'condor01.rcf.bnl.gov:9662', 'rhphenix',
             set(['CPU_Type', 'Job_Type', 'CPU_Experiment'])),
    'star':
        Pool('star', 'condor02.rcf.bnl.gov:9664', 'rhstar',
             set(['CPU_Type', 'Job_Type', 'CPU_Experiment']))
}
