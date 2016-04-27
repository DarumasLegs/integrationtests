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
A general class dedicated to Physical Disk logic
"""

import time
from ci.tests.general.connection import Connection
from ci.tests.general.general import General
from ci.tests.general.general_storagerouter import GeneralStorageRouter
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.lists.disklist import DiskList
from ovs.dal.lists.diskpartitionlist import DiskPartitionList
from ovs.extensions.generic.sshclient import SSHClient


class GeneralDisk(object):
    """
    A general class dedicated to Physical Disk logic
    """
    @staticmethod
    def get_disk(guid):
        """
        Retrieve a Disk
        :param guid: Guid of the Disk
        :return: Disk DAL object
        """
        return Disk(guid)

    @staticmethod
    def get_disks():
        """
        Retrieve all physical disks
        :return: Data-object Disk list
        """
        return DiskList.get_disks()

    @staticmethod
    def get_disk_by_devicename(storagerouter, device_name):
        """
        Retrieve a disk based on its devicename
        :param storagerouter: Storage Router of the disk
        :param device_name: Device name of the disk
        :return: Disk DAL object
        """
        if device_name.startswith('/dev/'):
            device_name = device_name.replace('/dev/', '')

        for disk in GeneralDisk.get_disks():
            if disk.name == device_name and disk.storagerouter == storagerouter:
                return disk
        raise ValueError('No disk found with devicename {0}'.format(device_name))

    @staticmethod
    def get_disk_partition(guid):
        """
        Retrieve a Disk Partition
        :param guid: Guid of the Disk Partition
        :return: Disk Partition DAL object
        """
        return DiskPartition(guid)

    @staticmethod
    def get_disk_partitions():
        """
        Retrieve all physical disk partitions
        :return: Data-object DiskPartition list
        """
        return DiskPartitionList.get_partitions()

    @staticmethod
    def get_unused_disks():
        """
        Retrieve all disks not in use
        :return: List of disks not being used
        """
        # @TODO: Make this call possible on all nodes, not only on node executing the tests
        all_disks = General.execute_command("""fdisk -l 2>/dev/null| awk '/Disk \/.*:/ {gsub(":","",$s);print $2}'""")[0].splitlines()
        out = General.execute_command("df -h | awk '{print $1}'")[0]

        return [d for d in all_disks if d not in out and not General.execute_command("fuser {0}".format(d))[0]]

    @staticmethod
    def get_physical_disks(client):
        """
        Retrieve physical disk information
        :param client: SSHClient object
        :type client: SSHClient

        :return: Physical disk information
        """
        disks_by_id = client.run('ls /dev/disk/by-id/').splitlines()
        part_info = client.run('lsblk --raw --noheadings --bytes --output name,type,size,rota').splitlines()
        dev_diskname_map = dict((client.file_read_link('/dev/disk/by-id/{0}'.format(entry)), entry) for entry in disks_by_id)

        hdds = dict()
        ssds = dict()
        for entry in part_info:
            disk_info = entry.split()
            disk_id = disk_info[0]  # sda
            disk_type = disk_info[1]  # disk, part, rom, ...
            disk_size = disk_info[2]  # size in bytes
            disk_rot = disk_info[3]   # 0 for SSD, 1 for HDD
            disk_dev = '/dev/{0}'.format(disk_id)
            if disk_type != 'disk':
                continue
            if disk_id[:2] in ['fd', 'sr', 'lo']:
                continue
            if disk_dev not in dev_diskname_map:
                continue

            if disk_rot == '0':
                ssds[disk_id] = {'size': disk_size, 'is_ssd': True, 'name': dev_diskname_map[disk_dev]}
            else:
                hdds[disk_id] = {'size': disk_size, 'is_ssd': False, 'name': dev_diskname_map[disk_dev]}
        return hdds, ssds

    @staticmethod
    def configure_disk(storagerouter, disk, offset, size, roles, partition=None):
        """
        Configure a disk
        :param storagerouter: Storage Router
        :param disk: Disk to configure
        :param partition: Partition on the disk
        :param offset: Offset of the partition
        :param size: Size of the partition
        :param roles: Roles to assign to the partition
        :return: None
        """
        api = Connection()
        api.execute_post_action(component='storagerouters',
                                guid=storagerouter.guid,
                                action='configure_disk',
                                data={'disk_guid': disk.guid,
                                      'offset': offset,
                                      'size': size,
                                      'roles': roles,
                                      'partition_guid': None if partition is None else partition.guid},
                                wait=True,
                                timeout=300)

    @staticmethod
    def append_disk_role(partition, roles_to_add):
        """
        Configure a disk
        :param partition: Disk partition
        :param roles_to_add: Roles to add to the disk
        :return: None
        """
        roles = partition.roles
        for role in roles_to_add:
            if role not in roles:
                roles.append(role)
        GeneralDisk.configure_disk(storagerouter=partition.disk.storagerouter,
                                   disk=partition.disk,
                                   partition=partition,
                                   offset=partition.offset,
                                   size=partition.size,
                                   roles=roles)

    @staticmethod
    def add_db_role(storagerouter):
        """
        Add a DB role to a Storage Router
        :param storagerouter: Storage Router
        :return: None
        """
        role_added = False
        db_partition = None
        for partition in GeneralDisk.get_disk_partitions():
            if partition.disk.storagerouter == storagerouter:
                if partition.mountpoint and '/mnt/ssd' in partition.mountpoint:
                    db_partition = partition
                if partition.mountpoint == '/' or partition.folder == '/mnt/storage':
                    GeneralDisk.append_disk_role(partition, ['DB'])
                    role_added = True
                    break
        # LVM partition present on the / mountpoint
        if db_partition is None and role_added is False:
            # disks havent been partitioned yet
            disks_to_partition = []
            disks = GeneralDisk.get_disks()
            if len(disks) == 1:
                if len(disks[0].partitions) == 0:
                    disks_to_partition = disks
            elif len(disks) > 1:
                disks_to_partition = [disk for disk in disks if disk.storagerouter == storagerouter and
                                      not disk.partitions_guids and disk.is_ssd]
            for disk in disks_to_partition:
                db_partition = GeneralDisk.partition_disk(disk)
            GeneralDisk.append_disk_role(db_partition, ['DB'])
        elif role_added is False:
            GeneralDisk.append_disk_role(db_partition, ['DB'])

    @staticmethod
    def partition_disk(disk):
        """
        Partition a disk
        :param disk: Disk DAL object
        :return: None
        """
        if len(disk.partitions) != 0:
            return disk.partitions[0]

        GeneralDisk.configure_disk(storagerouter=disk.storagerouter,
                                   disk=disk,
                                   offset=0,
                                   size=disk.size,
                                   roles=[])
        disk = GeneralDisk.get_disk(guid=disk.guid)
        assert len(disk.partitions) >= 1, "Partitioning failed for disk:\n {0} ".format(disk.name)
        return disk.partitions[0]

    @staticmethod
    def unpartition_disk(disk, partitions=None):
        """
        Return disk to RAW state
        :param disk: Disk DAL object
        :param partitions: Partitions DAL object list
        :return: None
        """
        if partitions is None:
            partitions = disk.partitions
        else:
            for partition in partitions:
                if partition not in disk.partitions:
                    raise RuntimeError('Partition {0} does not belong to disk {1}'.format(partition.mountpoint, disk.name))
        if len(disk.partitions) == 0:
            return

        root_client = SSHClient(disk.storagerouter, username='root')
        for partition in partitions:
            General.unmount_partition(root_client, partition)
        root_client.run("parted -s /dev/{0} mklabel gpt".format(disk.name))
        GeneralStorageRouter.sync_with_reality(disk.storagerouter)
        counter = 0
        timeout = 60
        while counter < timeout:
            time.sleep(1)
            disk = GeneralDisk.get_disk(guid=disk.guid)
            if len(disk.partitions) == 0:
                break
            counter += 1
        if counter == timeout:
            raise RuntimeError('Removing partitions failed for disk:\n {0} '.format(disk.name))

    @staticmethod
    def add_read_write_scrub_roles(storagerouter):
        """
        Add READ, WRITE, SCRUB roles to a Storage Router
        :param storagerouter: Storage Router
        :return: None
        """
        # @TODO: Remove this function and create much more generic functions which allow us much more configurability
        hdds = []
        ssds = []
        for disk in GeneralDisk.get_disks():
            if disk.storagerouter != storagerouter or [part for part in disk.partitions if 'BACKEND' in part.roles] or disk.partitions:
                continue
            if disk.is_ssd is True:
                ssds.append(disk)
            else:
                hdds.append(disk)

        add_read = not GeneralStorageRouter.has_roles(storagerouter=storagerouter, roles=['READ'])
        add_scrub = not GeneralStorageRouter.has_roles(storagerouter=storagerouter, roles=['SCRUB'])
        add_write = not GeneralStorageRouter.has_roles(storagerouter=storagerouter, roles=['WRITE'])

        if (add_read is True or add_scrub is True or add_write is True) and len(hdds) == 0 and len(ssds) == 0:
            raise ValueError('Not enough disks available to add required roles')

        disks_to_partition = {}
        if add_scrub is True:
            disk = hdds[0] if len(hdds) > 0 else ssds[0]
            disks_to_partition[disk] = ['SCRUB']

        if add_read is True:
            if len(ssds) > 1:
                disk = ssds.pop(0)  # Pop because we don't want to re-use for WRITE role
            elif len(ssds) == 1:
                disk = ssds[0]
            else:
                disk = hdds[0]

            if disk in disks_to_partition:
                disks_to_partition[disk].append('READ')
            else:
                disks_to_partition[disk] = ['READ']

        if add_write is True:
            if len(ssds) > 0:
                disk = ssds[0]
            else:
                disk = hdds[0]

            if disk in disks_to_partition:
                disks_to_partition[disk].append('WRITE')
            else:
                disks_to_partition[disk] = ['WRITE']

        for disk, roles in disks_to_partition.iteritems():
            GeneralDisk.partition_disk(disk)
            GeneralDisk.append_disk_role(disk.partitions[0], roles)
