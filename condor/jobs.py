#!/usr/bin/python
# *************  MODULE NOT USED!  *******************

import time
from collections import defaultdict
import logging

import htcondor

logger = logging.getLogger(__name__)

bins = [(300,       'recent'),
        (3600,      'one_hour'),
        (3600 * 4,    'four_hours'),
        (3600 * 8,    'eight_hours'),
        (3600 * 24,   'one_day'),
        (3600 * 24 * 2, 'two_days'),
        (3600 * 24 * 7, 'one_week')]


def find_bin(value, bins):
    for b in bins:
        if value < b[0]:
            return b[1]
    return "longer"


def job_metrics(job_classad, schedd_name):
    """
    Returns a list of base metrics for the given job.
    """
    counters = []

    # exp_name = job_classad.get(
    # "AccountingGroup", "group_none").split(".")[0][6:]
    exp_name = job_classad.get("RealExperiment", "UnknownExp")
    user_name = job_classad.get("Owner", "UnknownOwner")

    if job_classad["JobUniverse"] == 7:
        counters = [".dag.totals"]
    elif job_classad["JobStatus"] == 1:
        counters = [".idle.totals"]
        counters.append(".idle.unknown")
    elif job_classad["JobStatus"] == 2:
        counters = [".running.totals"]
        counters.append(".running.sites." + exp_name)
    elif job_classad["JobStatus"] == 5:
        counters = [".held.totals"]
    else:
        counters = [".unknown.totals"]

    metrics = []
    for counter in counters:
        metrics.append("totals" + counter)
        metrics.append("experiments." + exp_name + ".totals" + counter)
        metrics.append("experiments." + exp_name +
                       ".users." + user_name + counter)
        metrics.append("users." + user_name + counter)
        metrics.append("schedds." + schedd_name + ".totals" + counter)
        metrics.append("schedds." + schedd_name +
                       ".experiments." + exp_name + ".totals" + counter)
        metrics.append("schedds." + schedd_name + ".experiments." +
                       exp_name + ".users." + user_name + counter)
    return metrics


def job_walltime(job_classad):
    return job_classad.get('RemoteWallclockTime')


def job_cputime(job_classad):
    return job_classad.get("RemoteUserCpu", 0) + job_classad.get("RemoteSysCpu", 0)


def job_bin(job_classad):
    bin = None
    if job_classad["JobStatus"] == 1:
        if "QDate" in job_classad:
            qage = job_classad["ServerTime"] - job_classad["QDate"]
            bin = ".count_" + find_bin(qage, bins)
        else:
            bin = ".count_unknown"
    elif job_classad["JobStatus"] == 2:
        walltime = job_walltime(job_classad)
        if walltime > 0:
            bin = ".count_" + find_bin(walltime, bins)
        else:
            bin = ".count_unknown"
    elif job_classad["JobStatus"] == 5:
        if "EnteredCurrentStatus" in job_classad:
            holdage = job_classad["ServerTime"] - \
                job_classad["EnteredCurrentStatus"]
            bin = ".count_holdage_" + find_bin(holdage, bins)
        else:
            bin = ".count_holdage_unknown"
    return bin


def get_job_count(self, retry_delay=30, max_retries=4):
    try:
        ads = self.local_schedds()
    except:
        logging.error(
            "Trouble getting pool {0} schedds.".format(self.pool))
        return None

    counts = defaultdict(int)
    for a in ads:
        logger.debug("Trying schedd: %s", a['Name'])
        if a.get('CollectorHost', 'None') != self.pool:
            continue
        retries = 0
        while retries < max_retries:
            try:
                schedd = htcondor.Schedd(a)
                constraint = True
                results = schedd.query(constraint, [
                    "ClusterId", "ProcId", "Owner", "AccountingGroup", "JobStatus", "JobUniverse",
                    "RealExperiment", "Experiment", "QDate", "ServerTime", "JobCurrentStartDate",
                    "RemoteUserCpu", "EnteredCurrentStatus", "RemoteSysCpu", "RequestMemory",
                    "ResidentSetSize_RAW", "RequestDisk", "DiskUsage_RAW", "RequestCpus"
                ])
            except:
                logging.warning("Trouble communicating with schedd {0}, retrying in {1}s.".format(
                    a['Name'], retry_delay))
                retries += 1
                results = None
                time.sleep(retry_delay)
                continue
            else:
                break

        if results is None:
            logging.error(
                "Trouble communicating with schedd {0}, giving up.".format(a['Name']))
            continue

        schedd_name = a["Name"].replace(".", "_").replace("@", "-")
        for r in results:
            metrics = job_metrics(r, schedd_name)
            for m in metrics:
                counts[m + ".count"] += 1

                bin = job_bin(r)
                if bin is not None:
                    counts[m + bin] += 1

                walltime = job_walltime(r)
                cputime = job_cputime(r)
                if walltime > 0 and cputime > 0:
                    counts[m + ".walltime"] += walltime
                    counts[m + ".cputime"] += cputime
                    counts[m + ".efficiency"] = max(
                        min(counts[m + ".cputime"] / counts[m + ".walltime"] * 100, 100), 0)
                    counts[m + ".wastetime"] = counts[m +
                                                      ".walltime"] - counts[m + ".cputime"]
                    if counts[m + ".count"] > 0:
                        counts[m + ".wastetime_avg"] = counts[m +
                                                              ".wastetime"] / counts[m + ".count"]

                if r["JobStatus"] == 2:
                    if "RequestCpus" in r:
                        counts[m + ".cpu_request"] += r.eval("RequestCpus")
                    if "RequestMemory" in r:
                        counts[
                            m + ".memory_request_b"] += r.eval("RequestMemory") * 1024 * 1024
                    if "ResidentSetSize_RAW" in r:
                        counts[
                            m + ".memory_usage_b"] += r.eval("ResidentSetSize_RAW") * 1024
                    if "RequestDisk" in r:
                        counts[
                            m + ".disk_request_b"] += r.eval("RequestDisk") * 1024
                    if "DiskUsage_RAW" in r:
                        counts[
                            m + ".disk_usage_b"] += r.eval("DiskUsage_RAW") * 1024

    return counts
