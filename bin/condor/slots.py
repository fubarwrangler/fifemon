#!/usr/bin/python
from collections import defaultdict
import logging

import htcondor

logger = logging.getLogger(__name__)


def sanitize(key):
    if key is None:
        return None
    return key.replace(".", "_").replace("@", "-").replace(" ", "_")


def get_pool_slots(pool, extras=[]):
    retries = 0
    logger.debug("Pool extras are: %s", extras)

    try:
        startd_ads = pool.query(htcondor.AdTypes.Startd, True,
                                ['SlotType', 'State', 'Name', 'SlotWeight',
                                 'Cpus', 'TotalSlotCpus', 'TotalCpus',
                                 'Disk', 'TotalSlotDisk', 'TotalDisk',
                                 'Memory', 'TotalSlotMemory', 'TotalMemory',
                                 'LoadAvg', 'TotalCondorLoadAvg', 'TotalLoadAvg',
                                 'AccountingGroup', 'RemoteGroup', 'RemoteOwner',
                                 'Owner'] + extras)
    except:
        logger.warning("trouble getting pool %s startds", pool.name)
        retries += 1
        startd_ads = None
        return {}

    if startd_ads is None:
        logger.error(
            "trouble getting pool {0} startds, giving up.".format(pool))
        return {}

    data = defaultdict(int)
    for a in startd_ads:
        slot_type = a.get('SlotType', 'Static')
        state = a.get('State', 'Unknown')

        if slot_type == 'Partitionable':
            if a['Cpus'] == 0 or a['Memory'] < 500 or a['Disk'] < 1048576:
                for k in ['TotalDisk', 'TotalSlotDisk',
                          'TotalMemory', 'TotalSlotMemory',
                          'TotalCpus', 'TotalSlotCpus',
                          'TotalLoadAvg', 'LoadAvg', 'TotalCondorLoadAvg']:

                    metric = '.'.join([slot_type, 'totals', k])
                    data[metric] += a[k]
                # slot is effectively fully utilized, reclassffy remaining resources
                slot_type = 'Dynamic'
                state = 'Unusable'
            else:
                for k in ['TotalDisk', 'TotalSlotDisk', 'Disk',
                          'TotalMemory', 'TotalSlotMemory', 'Memory',
                          'TotalCpus', 'TotalSlotCpus', 'Cpus',
                          'TotalLoadAvg', 'LoadAvg', 'TotalCondorLoadAvg']:
                    metric = '.'.join([slot_type, 'totals', k])
                    data[metric] += a[k]

        hierarchy = [slot_type, state]

        if state == 'Claimed':
            grp_default = 'rootgroup'
            group = grp_default
            if 'AccountingGroup' in a:
                group = '.'.join(a['AccountingGroup'].split('@')[0].split('.')[:-1])
            elif 'RemoteGroup' in a:
                group = a['RemoteGroup'] if a['RemoteGroup'] != '<none>' else grp_default
            owner = a.get('Owner', a.get('RemoteOwner', 'UnknownOwner').split('@')[-1])

            hierarchy += [sanitize(group), sanitize(owner)]
            for key in extras:
                hierarchy.append(a.get(key, 'undef'))

            for k in ['Disk', 'Memory', 'Cpus', 'LoadAvg']:
                metric = '.'.join(hierarchy + [k])
                data[metric] += a[k]

                metric = '.'.join([hierarchy[0], 'totals', k])
                data[metric] += a[k]

            metric = '.'.join(hierarchy + ['Weighted'])
            data[metric] += a.eval('SlotWeight')

            metric = '.'.join(hierarchy + ['NumSlots'])
            data[metric] += 1

        if state != 'Claimed' and slot_type != 'Partitionable':
            for k in ['Disk', 'Memory', 'Cpus']:
                metric = '.'.join(hierarchy[:2] + [k])
                data[metric] += a[k]
                metric = '.'.join([hierarchy[0], 'totals', k])
                data[metric] += a[k]
            metric = '.'.join(hierarchy[:2] + ['NumSlots'])
            data[metric] += 1

    return data

if __name__ == "__main__":
    import pprint
    import sys
    pprint.pprint(dict(get_pool_slots(sys.argv[1])))
