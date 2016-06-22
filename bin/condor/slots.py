#!/usr/bin/python
from collections import defaultdict
import re
import logging
import time
import fifemon

import htcondor

logger = logging.getLogger(__name__)


def sanitize(key):
    if key is None:
        return None
    return key.replace(".", "_").replace("@", "-").replace(" ", "_")


class Hierarchy(object):
    def __init__(self, node, children=None):
        self.data = node
        self.children = children


def ad_transform(ad):
    transforms = {
        'AccountingGroup': lambda x: '.'.join(x.split('@')[0].split('.')[:-1]),
        'SlotWeight': lambda x: x.eval(),
    }
    for key in transforms:
        if key in ad:
            ad[key] = transforms[key](ad[key])




def get_pool_slots(pool, retry_delay=30, max_retries=4):
    retries = 0

    try:
        startd_ads = pool.query(htcondor.AdTypes.Startd, True,
                                ['SlotType', 'State', 'Name', 'SlotWeight',
                                 'Cpus', 'TotalSlotCpus', 'TotalCpus',
                                 'Disk', 'TotalSlotDisk', 'TotalDisk',
                                 'Memory', 'TotalSlotMemory', 'TotalMemory',
                                 'LoadAvg', 'TotalCondorLoadAvg', 'TotalLoadAvg',
                                 'AccountingGroup', 'RemoteGroup', 'RemoteOwner',
                                 'RealExperiment', 'Experiment', ])
    except:
        logger.warning(
            "trouble getting pool {0} startds, retrying in {1}s.".format(pool, retry_delay))
        retries += 1
        startd_ads = None
        return {}

    if startd_ads is None:
        logger.error(
            "trouble getting pool {0} startds, giving up.".format(pool))
        return {}

    # Name, default value, not-found action (+ advance, 0 halt, - go back)
    base_ns = [('SlotType', 'Static', 0), ('State', 'Unknown', 1),
               ('AccountingGroup', 'Base', 1), ('Owner', -2)]
    grouping = {'type': ['Static', 'Dynamic'], 'foo': [], }
    aggregators = {'Cpus': 'State', 'Disk', 'LoadAvg', 'Memory', 'NumSlots', 'Weight'}

    for ad in startd_ads:
        ad = ad_transform(ad)
        for


    # data = defaultdict(int)
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
                    # metric = ".".join([slot_type, "startds", sanitize(a["Name"]), k])
                    # data[metric] = a[k]
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
                    # metric = ".".join([slot_type, "startds", sanitize(a["Name"]), k])
                    # data[metric] = a[k]
                    metric = ".".join([slot_type, "totals", k])
                    data[metric] += a[k]
        if state == "Claimed":
            (group, owner) = ("Root", "Unknown")
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

    return data

if __name__ == "__main__":
    import pprint
    import sys
    pprint.pprint(dict(get_pool_slots(sys.argv[1])))
