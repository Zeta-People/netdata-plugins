# -*- coding: utf-8 -*-
# Description: hdd space netdata python module
# Author: Dennis Lutter (lad1337)

from collections import OrderedDict
from functools import partial
import re

from base import ExecutableService

# default module values (can be overridden per job in `config`)
update_every = 60
priority = 60000
retries = 60

"""
flow:
1. __init__()
2. check()
3. create()
4. run()->_run_once()->_get_data()

"""


def format_dimemsion_id(prefix, disk):
    return '{}__{}'.format(prefix, disk.replace('/', '_'))


class Service(ExecutableService):
    def __init__(self, configuration=None, name=None):
        ExecutableService.__init__(self, configuration=configuration, name=name)
        self.disks = []
        self.definitions = OrderedDict()
        self.hdd_regex = re.compile(self.configuration.get('hdd_regex', 'md[0-9]+$'))
        self.command = 'df -P'
        self.chart_name = "df"

    @property
    def order(self):
        return self.definitions.keys()

    @order.setter
    def order(self, _):
        pass

    def _get_data(self):
        u"""Filesystem     1024-blocks    Used Available Capacity Mounted on
        /dev/loop0        20971520 1970384  17085360      11% /
        tmpfs              3822880       0   3822880       0% /dev
        tmpfs              3822880       0   3822880       0% /sys/fs/cgroup
        /dev/md3         976285620 1005888 975279732       1% /disk3
        /dev/md4         976285620 1005888 975279732      13% /disk3
        cgroup_root        3822880       0   3822880       0% /host/sys/fs/cgroup
        rootfs             3747528  598368   3149160      16% /etc/netdata
        /dev/loop0        20971520 1970384  17085360      11% /etc/hosts
        shm                  65536       0     65536       0% /dev/shm
        """
        raw = self._get_raw_data()[1:]

        data = {'drives': []}
        for line in raw:
            if not line.strip():
                continue

            parts = [p for p in map(unicode.strip, line.split(' ')) if p]
            capacity = int(parts[4][:-1])
            disk = parts[0]
            if not self.hdd_regex.search(disk):
                continue
            data['drives'].append(disk)

            def add_disk_sufix(prefix):
                return format_dimemsion_id(prefix, disk)

            data.update({
                add_disk_sufix('hdd_used'): int(parts[2]) / 1000000,
                add_disk_sufix('hdd_avail'): int(parts[3]) / 1000000,
                add_disk_sufix('hdd_used_percentage'): capacity,
                add_disk_sufix('hdd_avail_percentage'): 100 - capacity,
            })

        return data

    def check(self):
        check = ExecutableService.check(self)
        data = self._get_data()

        def create_line(prefix, disk):
            return [format_dimemsion_id(prefix, disk), disk, 'absolute']

        charts = [
            dict(id='hdd_avail', text='HDD space available in GB', unit='GB'),
            dict(id='hdd_used', text='HDD space used in GB', unit='GB'),
            dict(id='hdd_used_percentage', text='HDD space used in percent', unit='%'),
            dict(id='hdd_avail_percentage', text='HDD space available in percent', unit='%'),
        ]
        for chart in charts:
            id_ = chart['id']
            self.definitions[id_] = {
                'options': [chart['text'], 'Disks', chart['unit'], 'diskspace', 'df', 'line'],
                'lines': [create_line(id_, drives) for drives in data['drives']]
            }
        return check


if __name__ == '__main__':
    import sys
    import traceback
    import pdb


    def _print(lvl, *params):
        print "--", lvl + ":", " ".join(params)


    class TestMixin(object):
        # def _get_raw_data(self):
        #    return self._get_data.__doc__.splitlines()

        info = partial(_print, 'INFO')
        debug = partial(_print, 'DEBUG')
        error = partial(_print, 'ERROR')
        warning = partial(_print, 'WARNING')


    config = {
        'update_every': update_every,
        'priority': priority,
        'retries': retries,
    }
    TestService = type('Service', (TestMixin, Service), {})
    try:
        s = TestService(configuration=config, name='HDDSize')
        s.check()
        s.create()
        s.run()
    except Exception:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)
