#!/usr/bin/python
import logging
import time
import cPickle
import struct
import socket
import random
import sys

logger = logging.getLogger(__name__)


def sanitize_key(key):
    if key is None:
        return key
    replacements = {
        ".": "_",
        " ": "_",
    }

    for old, new in replacements.iteritems():
        key = key.replace(old, new)
    return key


class Graphite(object):
    def __init__(self, hosts="localhost", pickle_port=2004, send_data=True, shuffle=True):
        if ',' in hosts:
            self.graphite_hosts = [x.strip() for x in hosts.split(',')]
            if shuffle:
                random.shuffle(self.graphite_hosts)
        else:
            self.graphite_hosts = [hosts]

        self.graphite_pickle_port = pickle_port
        self.send_data = send_data

    def send_dict(self, namespace, data, timestamp=None):
        """send data contained in dictionary as {k: v} to graphite dataset
        $namespace.k with current timestamp"""
        if data is None:
            logger.warning("send_dict called with no data")
            return
        if timestamp is None:
            timestamp = time.time()
        post_data = []
        # turning data dict into [('$path.$key',($timestamp,$value)),...]]
        for k, v in sorted(data.iteritems()):
            t = (namespace+"."+k, (timestamp, v))
            post_data.append(t)
            logger.debug(str(t))

        for host in list(self.graphite_hosts):
            if self.batch_send(host, post_data):
                break
            else:
                self.graphite_hosts.remove(host)
        else:
            logger.critical("None of the hosts in %s could be used!", self.graphite_hosts)
            sys.exit(1)

    def batch_send(self, host, post_data, batch_size=1000):

        for i in xrange(len(post_data)//batch_size + 1):
            # pickle data
            payload = cPickle.dumps(post_data[i*batch_size:(i+1)*batch_size], protocol=2)
            header = struct.pack("!L", len(payload))
            message = header + payload
            # throw data at graphite, first that succeeds if a list...
            if self.send_data:
                try:
                    self._send(host, self.graphite_pickle_port, message)
                except:
                    return False
        return True

    @staticmethod
    def _send(host, port, data):
        logger.info("Sending metrics to %s:%d", host, port)
        s = socket.socket()
        try:
            s.connect((host, port))
            s.sendall(data)
        except socket.error as e:
            logger.error("unable to send data to graphite at %s:%d (%s)\n", host, port, e)
            raise
        finally:
            s.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    data = {'count1': 5, 'count2': 0.5}
    g = Graphite()
    g.send_dict('test', data, send_data=False)
