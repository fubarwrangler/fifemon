#!/usr/bin/python
from collections import defaultdict
from optparse import OptionParser
import time
import logging
import datetime
import ConfigParser
import pprint

import boto3

import fifemon

logger = logging.getLogger(__name__)

def get_ec2_instance_cpu(session, region,instance,end_time=None,period=300):
    if end_time is None:
        end_time=datetime.datetime.utcnow()
    cw = session.client('cloudwatch',region_name=region)
    response = cw.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[{'Name':"InstanceId",'Value':instance}],
        StartTime=end_time-datetime.timedelta(seconds=period*2),
        EndTime=end_time,
        Period=period,
        Statistics=['Average','Minimum','Maximum'],
        Unit='Percent')
    datapoints=response['Datapoints']
    r = {}
    if len(datapoints) > 0:
        datapoint=datapoints[-1]
        r['avg'] = datapoint['Average']
        r['min'] = datapoint['Minimum']
        r['max'] = datapoint['Maximum']
    else:
        logging.warning('no CPU utilization received for instance %s'%instance)
    return r

def get_ec2_instances(session, region):
    r = defaultdict(int)
    cpu = defaultdict(float)
    try:
        ec2 = session.resource('ec2',region)
        instances = ec2.instances.all()
        for i in instances:
            if i.placement['GroupName'] != "":
                group = fifemon.graphite.sanitize_key(i.placement['GroupName'])
            else:
                group = "none"
            base_metric = "{region}.{az}.{group}.{type}.{key}.{state}".format(
                    region = region,
                    az = fifemon.graphite.sanitize_key(i.placement['AvailabilityZone']),
                    group = group,
                    type = fifemon.graphite.sanitize_key(i.instance_type),
                    key = fifemon.graphite.sanitize_key(i.key_name),
                    state = i.state['Name'])
            r[base_metric+".count"] += 1
            if i.state['Name'] == 'running':
                cpu_usage = get_ec2_instance_cpu(session, region, i.instance_id)
                count = r[base_metric+".count"]
                if 'avg' in cpu_usage:
                    oldavg = r[base_metric+".cpu_avg"]
                    r[base_metric+".cpu_avg"] = (oldavg*(count-1) + cpu_usage['avg'])/count
                if 'min' in cpu_usage:
                    oldmin = r[base_metric+".cpu_min"]
                    r[base_metric+".cpu_min"] = min(oldmin, cpu_usage['min'])
                if 'max' in cpu_usage:
                    oldmax = r[base_metric+".cpu_max"]
                    r[base_metric+".cpu_max"] = max(oldmax,cpu_usage['max'])
    except Exception as e:
        logger.error('error communicating with AWS: %s'%e)
    return r

class AwsProbe(fifemon.Probe):
    def __init__(self, *args, **kwargs):
        self.regions = kwargs.pop('regions', ['us-west-2',])
        self.profiles = kwargs.pop('profiles', [None])

        super(AwsProbe, self).__init__(*args, **kwargs)

    def post(self):
        for profile in self.profiles:
            session = boto3.session.Session(profile_name = profile)
            for region in self.regions:
                data = get_ec2_instances(session, region)
                logger.info("queried AWS region {0}".format(region))
                if len(data) == 0:
                    continue
                if self.use_graphite:
                    try:
                        self.graphite.send_dict(self.namespace+".%s"%profile, 
                                data, send_data=(not self.test))
                    except Exception as e:
                        logging.error("error sending data to graphite: %s"%e)
                if self.use_influxdb:
                    self.influxdb_tags['account'] = "%s"%profile
                    try:
                        self.influxdb.send_dict(data, send_data=(not self.test),
                                schema="region.az.group.type.key.state.measurement",
                                tags=self.influxdb_tags)
                    except Exception as e:
                        logging.error("error sending data to influxdb: %s"%e)


def get_options():
    parser = OptionParser(usage="usage: %prog [options] [config file(s)]")
    parser.add_option('-t','--test',action="store_true",
            help="output data to stdout, don't send to graphite (implies --once)")
    parser.add_option('-1','--once',action="store_true",
            help="run once and exit")
    (cmd_opts,args) = parser.parse_args()

    config = ConfigParser.SafeConfigParser()
    config.read(['awsmonitor.cfg', 'etc/awsmonitor.cfg',
        '/etc/awsmonitor.cfg', '/etc/awsmonitor/awsmonitor.cfg'])
    config.read(args)

    def parse_tags(tags):
        r = {}
        if tags is None or tags == "":
            return r
        for k,v in [kv.split(":") for kv in tags.split(",")]:
            r[k] = v
        return r

    opts = {
        'regions':           config.get("AWS", "regions").split(","),
        'profiles':          config.get("AWS", "profiles").split(","),
        'namespace':         config.get("graphite", "namespace"),
        'meta_namespace':    config.get("graphite", "meta_namespace"),
        'use_graphite':      config.getboolean("graphite", "enable"),
        'graphite_host':     config.get("graphite", "host"),
        'graphite_port':     config.getint("graphite", "port"),
        'use_influxdb':      config.getboolean("influxdb", "enable"),
        'influxdb_host':     config.get("influxdb", "host"),
        'influxdb_port':     config.get("influxdb", "port"),
        'influxdb_db':       config.get("influxdb", "db"),
        'influxdb_tags':     parse_tags(config.get("influxdb", "tags")),
        'test':              cmd_opts.test or config.getboolean("probe", "test"),
        'once':              cmd_opts.once or config.getboolean("probe", "once"),
    }

    return opts


if __name__ == '__main__':
    opts = get_options()
    if opts['test']:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logging.basicConfig(level=loglevel,
            format="[%(asctime)s] %(levelname)s (%(name)s):  %(message)s")

    logger.info('Probe configuraion: \n'+pprint.pformat(opts))

    probe = AwsProbe(**opts)
    probe.run()
