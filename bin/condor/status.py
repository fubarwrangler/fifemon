#!/usr/bin/python
import logging
import time

import classad
import htcondor

logger = logging.getLogger(__name__)


def get_pool_status(pool, retry_delay=30, max_retries=4):
    coll =  htcondor.Collector(pool)

    daemons = {"schedds": htcondor.DaemonTypes.Schedd,
               "collectors": htcondor.DaemonTypes.Collector,
               "negotiators": htcondor.DaemonTypes.Negotiator}

    data = {
            "schema": "daemon.name.measurement",
            "metrics": {},
            }
    for daemon_type, daemon in daemons.iteritems():
        retries = 0
        while retries < max_retries:
            try:
                ads = coll.locateAll(daemon)
            except:
                logger.warning("trouble getting pool {0} {1} status, retrying in {2}s.".format(pool,daemon_type,retry_delay))
                ads = None
                retries += 1
                time.sleep(retry_delay)
            else:
                break
        if ads is None:
            logger.error("trouble getting pool {0} {1} status, giving up.".format(pool,daemon_type))
        else:
            for ad in ads:
                for k in ad:
                    if type(ad[k]) in [int,long,float]:
                        metric = ".".join([daemon_type, ad["Name"].replace(".","_").replace("@","-").replace(" ","_"), k])
                        data["metrics"][metric] = ad[k]
    return [data]

