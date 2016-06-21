#!/usr/bin/python
import logging
import time

import classad
import htcondor

logger = logging.getLogger(__name__)


def get_pool_priorities(pool, retry_delay=30, max_retries=4):
    coll = htcondor.Collector(pool)
    retries = 0
    while retries < max_retries:
        try:
            ad = coll.locate(htcondor.DaemonTypes.Negotiator)
            n = htcondor.Negotiator(ad)
            prio = n.getPriorities()
        except:
            logging.warning("Trouble communicating with pool {0} negotiator, retrying in {1}s.".format(
                pool, retry_delay))
            retries += 1
            prio = None
            time.sleep(retry_delay)
        else:
            break

    if prio is None:
        logging.error(
            "Trouble communicating with pool {0} negotiator, giving up.".format(pool))
        return {}

    data = {}
    for p in prio:
        if p['IsAccountingGroup']:
            continue
        a, schedd = p['Name'].split('@')
        # FIXME: ???
        parts = a.split('.')
        exp = parts[0].split("_")[-1]
        name = parts[-1]
        basename = "{0}.{1}.{2}".format(
            schedd.replace(".", "_"),
            exp,
            name)
        for metric in ["ResourcesUsed",
                       "AccumulatedUsage",
                       "WeightedAccumulatedUsage",
                       "Priority",
                       "WeightedResourcesUsed",
                       "PriorityFactor"]:
            data[basename + "." + metric] = p[metric]
    return data
