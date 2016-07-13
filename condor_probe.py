#!/usr/bin/env python
from optparse import OptionParser
import logging
import ConfigParser
import sys
import os

import condor

logger = logging.getLogger(__name__)

CFGFILE = os.environ.get('CPROBECFG', '/etc/condor/probe.cfg')

def get_options():
    parser = OptionParser(usage="usage: %prog [options] pool [probes to run...]")
    parser.add_option('-t', '--test', action="store_true", default=False,
                      help="output data to stdout, don't send (implies debug)")
    parser.add_option('-c', '--config', default=None,
                      help="alternate configuration file (defaults to "
                           "$CPROBECFG or /etc/condor/probe.cfg)")
    parser.add_option('-d', '--debug', action="store_true", default=False,
                      help="enable debug-logging")

    (cmd_opts, args) = parser.parse_args()

    cfg = ConfigParser.SafeConfigParser()
    loglevel = logging.DEBUG if (cmd_opts.debug or cmd_opts.test) else logging.INFO
    logging.basicConfig(level=loglevel, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    cfile = CFGFILE if not cmd_opts.config else cmd_opts.config
    logger.debug("Reading configuration file: %s", cfile)
    cfg.read(cfile)
    if len(args) < 1:
        parser.error("Need a pool-name argument")
        sys.exit(1)

    name = args[0]
    probes = args[1:]

    pool = {}
    pool['pool'] = name
    for k, v in cfg.items(name):
        if k in ('attrs', 'groups'):
            v = [x.strip() for x in v.split(',')]
        pool[k] = v

    opts = {
        'debug': cmd_opts.debug or cmd_opts.test,
        'test': cmd_opts.test,
        'graphite': cfg.get('graphite', 'graphite_host'),
        'port': cfg.getint('graphite', 'pickle_port'),
        'namespace': cfg.get('graphite', 'namespace') + '.' + name,
        'probes': probes,
    }

    return opts, pool

if __name__ == '__main__':
    opts, pool = get_options()
    condor.CondorProbe(pool, **opts).run()
