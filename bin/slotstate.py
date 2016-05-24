#!/usr/bin/python
import logging
import json
import datetime
import time
from optparse import OptionParser
import math

import htcondor

logger = logging.getLogger(__name__)


class Slots(object):

    def __init__(self, pool="localhost"):
        self.pool = pool
        self.collector = htcondor.Collector(pool)

    def get_stats(self, retry_delay=30, max_retries=4, constraint=None, extra_stats=None):

        stats = ["Name", "State", "SlotType",
                 "GLIDEIN_Site", "GLIDEIN_Resource_Name", "GLIDEIN_ResourceName",
                 "DaemonStartTime", "GLIDEIN_ToDie", "MyCurrentTime",
                 "Disk", "Memory", "Cpus",
                 "LoadAvg", "TotalCondorLoadAvg"]
        if extra_stats:
            stats += extra_stats

        if constraint is None:
            constraint = "is_glidein"
        else:
            constraint = "is_glidein && (%s)" % constraint

        retries = 0
        while retries < max_retries:
            try:
                ads = self.collector.query(
                    htcondor.AdTypes.Startd, constraint, stats)
            except:
                logger.warning("Trouble getting pool {0} startds, retrying in {1}s.".format(
                    self.pool, retry_delay))
                retries += 1
                ads = None
                time.sleep(retry_delay)
            else:
                break

        if ads is None:
            logger.error(
                "Trouble getting pool {0} startds, giving up.".format(self.pool))
            return

        logger.info("Processing slots")

        for r in ads:
            rdict = {
                "timestamp":            datetime.datetime.utcnow().isoformat(),
                "pool":                 self.pool,
            }
            for s in stats:
                if s in r:
                    rdict[s] = r.eval(s)
            # convert
            for s in ["DaemonStartTime", "GLIDEIN_ToDie"]:
                if s in rdict:
                    rdict[s] = datetime.datetime.fromtimestamp(
                        rdict[s]).isoformat()
            if "Memory" in rdict:
                memory = int(rdict["Memory"]) * 1024 * 1024
                rdict["Memory"] = memory
                rdict["Memory_mb"] = math.floor(memory / 1024 / 1024)
                rdict["Memory_gb"] = math.floor(memory / 1024 / 1024 / 1024)
            if "Disk" in rdict:
                rdict["Disk"] = int(rdict["Disk"]) * 1024
            # calcs
            if "GLIDEIN_ToDie" in r and "MyCurrentTime" in r:
                time_left = r["GLIDEIN_ToDie"] - r["MyCurrentTime"]
                rdict["time_left"] = time_left
                rdict["time_left_hours"] = math.floor(time_left / 3600)
                rdict["time_left_8hours"] = math.floor(
                    time_left / 3600 / 8) * 8
                rdict["time_left_days"] = math.floor(time_left / 3600 / 24)
            # handle inconsistent resource names
            if "GLIDEIN_Resource_Name" in r:
                rdict["GLIDEIN_ResourceName"] = r["GLIDEIN_Resource_Name"]
            # replace FNAL site name with resource name
            if rdict.get('GLIDEIN_Site') == "FNAL":
                rdict["GLIDEIN_Site"] = rdict.get(
                    "GLIDEIN_ResourceName", "FNAL")
            yield rdict


def get_options():
    parser = OptionParser(usage="usage: %prog [options] pool")
    parser.add_option('--pool', default="localhost",
                      help="condor pool to query")
    parser.add_option('--constraint', default=True,
                      help="limit condor query")
    parser.add_option('--interval', default=300,
                      help="run interval (s)")
    parser.add_option('-d', '--debug', action="store_true",
                      help="enable debug logging")
    parser.add_option('-1', '--once', action="store_true",
                      help="run once and exit")
    (opts, args) = parser.parse_args()
    return opts

if __name__ == "__main__":
    opts = get_options()
    loglevel = logging.INFO
    if opts.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel,
                        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    slots = Slots(opts.pool)
    while True:
        start = time.time()
        logger.info("querying pool %s" % opts.pool)
        cnt = 0
        for s in slots.get_stats(constraint=opts.constraint, extra_stats=["GLIDECLIENT_group"]):
            cnt += 1
            print json.dumps(s)
        end = time.time()
        logger.info("processed %d slots in %ds" % (cnt, end - start))
        if opts.once:
            break
        sleep = max(opts.interval - (end - start), 0)
        logger.info("sleeping %ds" % (sleep))
        time.sleep(sleep)
