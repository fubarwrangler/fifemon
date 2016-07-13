#!/usr/bin/python
import logging
import htcondor
import time

logger = logging.getLogger(__name__)


def get_pool_priorities(self):

    try:
        ad = self.get_daemons('negotiator')[0]
        n = htcondor.Negotiator(ad)
        prio = n.getPriorities(False)
    except:
        logging.exception('Trouble communicating with %s negotiator', self.name)
        return {}

    now = time.time()
    data = {}
    for p in prio:
        if p['IsAccountingGroup'] or now - p['LastUsageTime'] > 3600 * 24 * 60:
            continue

        ag = p['AccountingGroup']
        if ag == '<none>':
            ag = 'nogroup'
            user = p['Name'].replace('.', '_')
        else:
            user = p['Name'][len(ag) + 1:]

        username, domain = (x.replace('.', '_') for x in user.split('@'))

        ag = ag[ag.startswith('group_') and len('group_'):]

        basename = '{0}.{1}.{2}'.format(domain, ag.replace('.', '_'), username)

        for metric in ['ResourcesUsed',
                       'AccumulatedUsage',
                       'WeightedAccumulatedUsage',
                       'Priority',
                       'WeightedResourcesUsed',
                       'PriorityFactor']:
            data[basename + '.' + metric] = p[metric]
    return data
