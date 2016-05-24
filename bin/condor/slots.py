#!/usr/bin/python
from collections import defaultdict
import re
import logging
import time

import classad
import htcondor

logger = logging.getLogger(__name__)


def sanitize(key):
    if key is None:
        return None
    return key.replace(".", "_").replace("@", "-").replace(" ", "_")


def get_pool_resource_utilization(pool, retry_delay=30, max_retries=4):
    coll = htcondor.Collector(pool)
    retries = 0
    while retries < max_retries:
        try:
            schedd_ads = coll.locateAll(htcondor.DaemonTypes.Schedd)
        except:
            logger.warning(
                "trouble getting pool {0} schedds, retrying in {1}s.".format(pool, retry_delay))
            retries += 1
            schedd_ads = None
            time.sleep(retry_delay)
        else:
            break

    if schedd_ads is None:
        logger.error(
            "trouble getting pool {0} schedds, giving up.".format(pool))
        return {}

    memory_usage = 0
    disk_usage = 0
    for ad in schedd_ads:
        try:
            schedd = htcondor.Schedd(ad)
            results = schedd.query(
                'jobstatus==2', ['ResidentSetSize_RAW', 'DiskUsage_RAW'])
        except Exception as e:
            logger.error(e)
        else:
            for r in results:
                memory_usage += r.get('ResidentSetSize_RAW', 0)
                disk_usage += r.get('DiskUsage_RAW', 0)
    return {
        "MemoryUsage": memory_usage / 1024,
        "DiskUsage": disk_usage,
    }


def get_pool_slots(pool, retry_delay=30, max_retries=4):
    coll = htcondor.Collector(pool)
    retries = 0
    while retries < max_retries:
        try:
            #startd_ads = coll.locateAll(htcondor.DaemonTypes.Startd)
            startd_ads = coll.query(htcondor.AdTypes.Startd, True,
                                    ['SlotType', 'State', 'Name', 'SlotWeight',
                                     'Cpus', 'TotalSlotCpus', 'TotalCpus',
                                     'Disk', 'TotalSlotDisk', 'TotalDisk',
                                     'Memory', 'TotalSlotMemory', 'TotalMemory',
                                     'LoadAvg', 'TotalCondorLoadAvg', 'TotalLoadAvg',
                                     'AccountingGroup', 'RemoteGroup', 'RemoteOwner'])
        except:
            logger.warning(
                "trouble getting pool {0} startds, retrying in {1}s.".format(pool, retry_delay))
            retries += 1
            startd_ads = None
            time.sleep(retry_delay)
        else:
            break

    if startd_ads is None:
        logger.error(
            "trouble getting pool {0} startds, giving up.".format(pool))
        return {}

    data = defaultdict(int)
    load = defaultdict(float)
    for a in startd_ads:
        slot_type = a.get("SlotType", "Static")
        state = a.get("State", "Unknown")

        if slot_type == "Partitionable":
            if a["Cpus"] == 0 or a["Memory"] < 500 or a["Disk"] < 1048576:
                for k in ["TotalDisk", "TotalSlotDisk",
                          "TotalMemory", "TotalSlotMemory",
                          "TotalCpus", "TotalSlotCpus",
                          "TotalLoadAvg", "LoadAvg", "TotalCondorLoadAvg"]:
                    #metric = ".".join([slot_type, "startds", sanitize(a["Name"]), k])
                    #data[metric] = a[k]
                    metric = ".".join([slot_type, "totals", k])
                    data[metric] += a[k]
                # slot is effectively fully utilized, reclassffy remaining
                # resources
                slot_type = "Dynamic"
                state = "Unusable"
            else:
                for k in ["TotalDisk", "TotalSlotDisk", "Disk",
                          "TotalMemory", "TotalSlotMemory", "Memory",
                          "TotalCpus", "TotalSlotCpus", "Cpus",
                          "TotalLoadAvg", "LoadAvg", "TotalCondorLoadAvg"]:
                    #metric = ".".join([slot_type, "startds", sanitize(a["Name"]), k])
                    #data[metric] = a[k]
                    metric = ".".join([slot_type, "totals", k])
                    data[metric] += a[k]
        if state == "Claimed":
            (group, owner) = ("Unknown", "Unknown")
            if "AccountingGroup" in a:
                m = re.match(r'group_(\S+)\.(\S+)@\S+$', a["AccountingGroup"])
                if m:
                    group, owner = m.groups()
            if group == "Unknown" and "RemoteGroup" in a:
                group = a["RemoteGroup"]
                if group == "<none>":
                    group = "None"
            if owner == "Unknown" and "RemoteOwner" in a:
                owner = a["RemoteOwner"].split("@")[0]

            for k in ["Disk", "Memory", "Cpus", "LoadAvg"]:
                metric = ".".join(
                    [slot_type, state, sanitize(group), sanitize(owner), k])
                data[metric] += a[k]
                metric = ".".join([slot_type, "totals", k])
                data[metric] += a[k]
            metric = ".".join([slot_type, state, sanitize(
                group), sanitize(owner), "Weighted"])
            data[metric] += a.eval("SlotWeight")
            metric = ".".join([slot_type, state, sanitize(
                group), sanitize(owner), "NumSlots"])
            data[metric] += 1
        if state != "Claimed" and slot_type != "Partitionable":
            for k in ["Disk", "Memory", "Cpus"]:
                metric = ".".join([slot_type, state, k])
                data[metric] += a[k]
                metric = ".".join([slot_type, "totals", k])
                data[metric] += a[k]
            metric = ".".join([slot_type, state, "NumSlots"])
            data[metric] += 1
    for k, v in get_pool_resource_utilization(pool, retry_delay, max_retries).iteritems():
        metric = ".".join(["jobs", "totals", k])
        data[metric] = v

    return data


def get_pool_glidein_slots(pool, retry_delay=30, max_retries=4):
    coll = htcondor.Collector(pool)
    retries = 0
    while retries < max_retries:
        try:
            startd_ads = coll.query(htcondor.AdTypes.Startd, 'is_glidein==True',
                                    ['GLIDEIN_Site', 'GLIDEIN_Resource_Name', 'GLIDEIN_ResourceName', 'State',
                                     'DaemonStartTime', 'Disk', 'Memory', 'Cpus'])
        except:
            logger.warning(
                "trouble getting pool {0} startds, retrying in {1}s.".format(pool, retry_delay))
            retries += 1
            startd_ads = None
            time.sleep(retry_delay)
        else:
            break

    if startd_ads is None:
        logger.error(
            "trouble getting pool {0} startds, giving up.".format(pool))
        return {}

    data = defaultdict(int)
    load = defaultdict(float)
    for a in startd_ads:
        site = a.get("GLIDEIN_Site", "Unknown")
        resource = a.get("GLIDEIN_Resource_Name", a.get(
            "GLIDEIN_ResourceName", "Unknown"))
        state = a.get("State", "Unknown")
        if (time.time() - a.get("DaemonStartTime", time.time())) < 300:
            state = "New"

        metrics = [".".join(["glideins", "totals", "NumSlots"]),
                   ".".join(["glideins", state, "totals", "NumSlots"]),
                   ".".join(["glideins", state, "sites",
                             site, "totals", "NumSlots"]),
                   ".".join(["glideins", state, "sites", site, "resources", resource, "NumSlots"])]
        for m in metrics:
            data[m] += 1

        for k in ["Disk", "Memory", "Cpus"]:
            metrics = [".".join(["glideins", "totals", k]),
                       ".".join(["glideins", state, "totals", k]),
                       ".".join(
                           ["glideins", state, "sites", site, "totals", k]),
                       ".".join(["glideins", state, "sites", site, "resources", resource, k])]
            for m in metrics:
                data[m] += a[k]

    return data

if __name__ == "__main__":
    import pprint
    pprint.pprint(dict(get_pool_slots()))
