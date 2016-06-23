#!/usr/bin/python
import logging

logger = logging.getLogger(__name__)


def get_pool_status(self):

    data = {}
    daemon_data = ((x, self.get_daemons(x)) for x in ('collector', 'negotiator', 'schedd'))
    for daemon_type, ads in daemon_data:

        if ads is None:
            logger.error('Failed to get pool %s %s ads', self.name, daemon_type)
            return {}

        for ad in ads:
            for k in ad:
                clean_name = ad['Name'].replace('.', '_').replace('@', '-').replace(' ', '_')
                if type(ad[k]) in [int, long, float]:
                    metric = '.'.join([daemon_type, clean_name, k])
                    data[metric] = ad[k]
    return data
