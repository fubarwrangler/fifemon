#!/usr/bin/python
import logging
import json
import datetime
import time
from optparse import OptionParser

import htcondor

logger = logging.getLogger(__name__)


class Jobs(object):
    def __init__(self, pool="localhost"):
        self.pool = pool
        self.collector = htcondor.Collector(pool)

    def job_walltime(self, job_classad):
        now = job_classad.get("ServerTime", 0)
        start = job_classad.get("JobCurrentStartDate", now)
        return now-start

    def job_cputime(self, job_classad):
        return job_classad.get("RemoteUserCpu", 0)

    def get_job_stats(self, retry_delay=30, max_retries=4, constraint=True, extra_stats=None):

        stats = ["ClusterId", "ProcId", "JobStatus", "JobUniverse",
                 "QDate", "ServerTime", "JobCurrentStartDate", "RemoteUserCpu",
                 "EnteredCurrentStatus"]
        if extra_stats:
            stats += extra_stats

        try:
            ads = self.collector.locateAll(htcondor.DaemonTypes.Schedd)
        except:
            logger.error("Trouble getting pool {0} schedds.".format(self.pool))
            return

        for a in ads:
            logger.info("Querying jobs from schedd %s"%a['Name'])
            retries = 0
            while retries < max_retries:
                try:
                    schedd = htcondor.Schedd(a)
                    results = schedd.query(constraint, stats)
                except:
                    logger.warning("Trouble communicating with schedd {0}, retrying in {1}s.".format(a['Name'], retry_delay))
                    retries += 1
                    results = None
                    time.sleep(retry_delay)
                    continue
                else:
                    break

            if results is None:
                logger.error("Trouble communicating with schedd {0}, giving up.".format(a['Name']))
                continue

            logger.info("Processing jobs")

            for r in results:
                if r["JobUniverse"] == 7:
                    # skip dagman jobs
                    continue

                jobid = "%s.%s@%s" % (r["ClusterId"], r["ProcId"], a["Name"])

                rdict = {
                    "timestamp":            datetime.datetime.utcnow().isoformat(),
                    "pool":                 self.pool,
                    "schedd":               a["Name"],
                    "jobid":                jobid,
                    "cluster":              r["ClusterId"],
                    "process":              r["ProcId"],
                    "status":               r["JobStatus"],
                    "submit_date":          datetime.datetime.fromtimestamp(r["QDate"]).isoformat(),
                }
                if r["JobStatus"] == 2:
                    rdict["start_date"] = datetime.datetime.fromtimestamp(r.get("JobCurrentStartDate", time.time())).isoformat()
                    rdict["walltime"] = self.job_walltime(r)
                    rdict["cputime"] = self.job_cputime(r)
                    if rdict["walltime"] > 0:
                        rdict["efficiency"] = rdict["cputime"]/rdict["walltime"]
                elif r["JobStatus"] == 5:
                    rdict["hold_date"] = datetime.datetime.fromtimestamp(r.get("EnteredCurrentStatus", time.time())).isoformat()
                for s in extra_stats:
                    if s in r:
                        rdict[s] = r.eval(s)
                yield rdict


def calc_stats(s):
    request_mem = s.get("RequestMemory", 1)
    if request_mem > 0:
        max_mem = s.get("ResidentSetSize_RAW", 0)
        s["memory_ratio"] = 1.0*max_mem/1024.0/request_mem
    request_disk = s.get("RequestDisk", 1)
    if request_disk > 0:
        max_disk = s.get("DiskUsage_RAW", 0)
        s["disk_ratio"] = 1.0*max_disk/request_disk
    request_time = s.get("JOB_EXPECTED_MAX_LIFETIME", 0)
    if request_time > 0:
        max_time = s.get("walltime", 0)
        s["time_ratio"] = 1.0*max_time/request_time
    return s


def get_options():
    parser = OptionParser(usage="usage: %prog [options] pool")
    parser.add_option('--pool', default="localhost", help="condor pool to query")
    parser.add_option('--constraint', default=True, help="limit condor query")
    parser.add_option('--interval', default=300, help="run interval (s)")
    parser.add_option('-1','--once', action="store_true", help="run once and exit")
    parser.add_option('-d', '--debug', action="store_true", help="enable debug logging")
    (opts, args) = parser.parse_args()
    return opts


if __name__ == "__main__":

    opts = get_options()
    loglevel = logging.INFO
    if opts.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    j = Jobs(opts.pool)

    while True:
        start = time.time()
        logger.info("querying pool %s", opts.pool)
        cnt = 0
        for s in j.get_job_stats(constraint=opts.constraint, extra_stats=[
                "Owner", "JobSub_Group", "AccountingGroup", "JobsubClientKerberosPrincipal",
                "DESIRED_usage_model", "DESIRED_Sites",
                "NumRestarts", "NumJobStarts", "RemoteUserCpu",
                "RequestMemory", "RequestDisk", "RequestCpus",
                "ResidentSetSize_RAW", "DiskUsage_RAW",
                "JOB_EXPECTED_MAX_LIFETIME",
                "HoldReason", "HoldReasonCode", "HoldReasonSubcode",
                "MATCH_GLIDEIN_Site"]):
            cnt += 1
            s = calc_stats(s)
            print json.dumps(s)
        if opts.once:
            break
        end = time.time()
        sleep = max(opts.interval-(end-start), 0)
        logger.info("processed %d jobs in %ds, sleeping %ds", (cnt, end-start, sleep))
        time.sleep(sleep)
