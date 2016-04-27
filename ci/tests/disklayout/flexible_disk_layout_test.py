# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Flexible disk layout testsuite
"""
import logging
from ci.tests.general.general import General
from ci.tests.general.general_disk import GeneralDisk
from ci.tests.general.general_storagerouter import GeneralStorageRouter
from ci.tests.general.general_vdisk import GeneralVDisk
from nose.tools import assert_equal
from nose.plugins.skip import SkipTest
from ovs.extensions.generic.sshclient import SSHClient


class ContinueTesting(object):
    """
    Class indicating whether the tests should go on
    """
    def __init__(self):
        self.state = True
        pass

    def set_state(self, state):
        """
        Update the state
        :param state: New state to set
        :return: None
        """
        self.state = state


class TestFlexibleDiskLayout(object):
    """
    Flexible disk layout testsuite
    """
    continue_testing = ContinueTesting()

    logger = logging.getLogger('test_flexible_disk_layout')

    #########
    # TESTS #
    #########

    @staticmethod
    def fdl_0001_match_model_with_reality_test():
        """
        FDL-0001 - disks in ovs model should match actual physical disk configuration
        """
        if TestFlexibleDiskLayout.continue_testing.state is False:
            raise SkipTest('Test suite signaled to stop')
        GeneralStorageRouter.sync_with_reality()

        physical_disks = dict()
        modelled_disks = dict()
        loops = dict()

        TestFlexibleDiskLayout.logger.setLevel('INFO')
        for storagerouter in GeneralStorageRouter.get_storage_routers():
            root_client = SSHClient(storagerouter, username='root')
            hdds, ssds = GeneralDisk.get_physical_disks(client=root_client)

            loops[storagerouter] = General.get_loop_devices(client=root_client)
            physical_disks[storagerouter] = hdds
            physical_disks[storagerouter].update(ssds)

        for disk in GeneralDisk.get_disks():
            if disk.storagerouter not in modelled_disks:
                modelled_disks[disk.storagerouter] = dict()
            if disk.name not in loops[disk.storagerouter]:
                modelled_disks[disk.storagerouter][disk.name] = {'is_ssd': disk.is_ssd}

        TestFlexibleDiskLayout.logger.info('PDISKS: {0}'.format(physical_disks))
        TestFlexibleDiskLayout.logger.info('MDISKS: {0}'.format(modelled_disks))

        assert_equal(first=set(modelled_disks),
                     second=set(physical_disks),
                     msg='Difference in storagerouters found')

        for storagerouter, physical_disk_info in physical_disks.iteritems():
            modelled_disk_info = modelled_disks[storagerouter]
            assert_equal(first=set(physical_disk_info),
                         second=set(modelled_disk_info),
                         msg='Nr of modelled/physical disks differs for storagerouter {0}:\nPhysical: {1}\nModel: {2}'.format(storagerouter.name,
                                                                                                                              physical_disk_info.keys(),
                                                                                                                              modelled_disk_info.keys()))

            for disk_name, single_physical_disk_info in physical_disk_info.iteritems():
                single_model_disk_info = modelled_disk_info[disk_name]
                assert 'is_ssd' in single_model_disk_info, 'Key "is_ssd" is not present in model info for disk {0}'.format(disk_name)
                assert 'is_ssd' in single_physical_disk_info, 'Key "is_ssd" is not present in physical info for disk {0}'.format(disk_name)
                assert_equal(first=single_physical_disk_info['is_ssd'],
                             second=single_model_disk_info['is_ssd'],
                             msg='SSD flag for Disk {0} on Storage Router {1} differs in model, value should be {2}'.format(disk_name,
                                                                                                                            storagerouter.name,
                                                                                                                            single_physical_disk_info['is_ssd']))

    @staticmethod
    def fdl_0002_add_remove_partition_with_role_and_crosscheck_model_test():
        """
        FDL-0002 - create/remove disk partition using full disk and verify ovs model
            - look for an unused disk
            - add a partition using full disk and assign a DB role to the partition
            - validate ovs model is correctly updated with DB role
            - cleanup that partition
            - verify ovs model is correctly updated
        """
        if TestFlexibleDiskLayout.continue_testing.state is False:
            raise SkipTest('Test suite signaled to stop')

        my_sr = GeneralStorageRouter.get_local_storagerouter()

        unused_disks = GeneralDisk.get_unused_disks()
        if not unused_disks:
            raise SkipTest("At least one unused disk should be available for partition testing")

        all_disks = dict(('/dev/{0}'.format(disk.name), disk) for disk in GeneralDisk.get_disks() if disk.storagerouter == my_sr)
        print all_disks.keys()

        # check no partitions are modelled for unused disks
        partitions = GeneralDisk.get_disk_partitions()
        partitions_detected = False
        disk_guid = ''
        for path in unused_disks:
            disk_guid = all_disks[path].guid
            for partition in partitions:
                if partition.disk_guid == disk_guid:
                    partitions_detected = True
        assert partitions_detected is False, 'Existing partitions detected on unused disks!'

        # try partition a disk using it's full reported size
        disk = all_disks[unused_disks[0]]
        GeneralDisk.configure_disk(storagerouter=my_sr,
                                   disk=disk,
                                   offset=0,
                                   size=int(disk.size),
                                   roles=['DB'])

        # lookup partition in model
        mountpoint = None
        partitions = GeneralDisk.get_disk_partitions()
        for partition in partitions:
            if partition.disk_guid == disk.guid and 'DB' in partition.roles:
                mountpoint = partition.mountpoint
                break

        assert mountpoint, 'New partition was not detected in model'

        # cleanup disk partition
        cmd = 'umount {0}; rmdir {0}'.format(mountpoint)
        General.execute_command_on_node(my_sr.ip, cmd)

        cmd = 'parted -s {0} rm 1'.format(disk.path)
        General.execute_command_on_node(my_sr.ip, cmd)

        # wipe partition table to be able to reuse this disk in another test
        GeneralVDisk.write_to_volume(location=disk.path,
                                     count=64,
                                     bs='1M',
                                     input_type='zero')
        GeneralStorageRouter.sync_with_reality()

        # verify partition no longer exists in ovs model
        is_partition_removed = True
        partitions = GeneralDisk.get_disk_partitions()
        for partition in partitions:
            if partition.disk_guid == disk_guid and 'DB' in partition.roles:
                is_partition_removed = False
                break

        assert is_partition_removed is True,\
            'New partition was not deleted successfully from system/model!'
