# Copyright 2016 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from ci.tests.general.connection import Connection
from ci.tests.general.general import test_config
from ci.tests.general.logHandler import LogHandler
from ovs.dal.lists.vdisklist import VDiskList

logger = LogHandler.get('vdisks', name='vdisk')
logger.logger.propagate = False

template_source_folder = test_config.get('vmachine', 'template_source_folder')
template_image = test_config.get('vmachine', 'template_image')
template_target_folder = test_config.get('vmachine', 'template_target_folder')

TEMPLATE_SERVERS = {"loch": "http://sso-qpackages-loch.cloudfounders.com/templates/openvstorage", "axs": "http://172.20.3.8/templates/openvstorage"}
TIMEOUT = 70
TIMER_STEP = 5

api = Connection.get_connection()


class GeneralVDisk(object):

    # @TODO: combine with the other methods implemented in https://github.com/openvstorage/integrationtests/pull/164
    @staticmethod
    def create_raw_vdisk_from_template(vpool_name, vdisk_name, client, wait=True):
        logger.info("Starting RAW disk creation from template")
        client.run('qemu-img convert -O raw {0}{1} /mnt/{2}/{3}.raw'.format(template_target_folder, template_image, vpool_name, vdisk_name))
        if wait:
            counter = TIMEOUT / TIMER_STEP
            while counter > 0:
                vdisk_list = api.get_component_by_name('vdisks', vdisk_name)
                if vdisk_list:
                    counter = 0
                else:
                    counter -= 1
                    time.sleep(TIMER_STEP)

    @staticmethod
    def remove_vdisk(vdisk_name, client, wait=True):
        if vdisk_name:
            vdisk_list = api.get_component_by_name('vdisks', vdisk_name)
            if vdisk_list:
                for vdisk in vdisk_list:
                    vpool = api.get_component_with_attribute('vpools', 'guid', vdisk['vpool_guid'])
                    logger.info("Removing disk {0} from {1} vpool".format(vdisk['name'], vpool['name']))
                    client.run("rm -rf /mnt/{0}/{1}*".format(vpool['name'], vdisk['name']))
            if wait:
                counter = TIMEOUT / TIMER_STEP
                while counter > 0:
                    time.sleep(TIMER_STEP)
                    vdisk_list = api.get_component_by_name('vdisks', vdisk_name)
                    if vdisk_list is None:
                        counter = 0
                    else:
                        counter -= 1

    @staticmethod
    def create_raw_vdisk(vpool_name, vdisk_name, client):
        logger.info("Starting RAW disk creation")
        client.run('truncate /mnt/{0}/{1}.raw --size 10000000'.format(vpool_name, vdisk_name))

    @staticmethod
    def download_template(server_location, client):
        logger.info("Getting template from {0}".format(server_location))
        client.run('wget -P {0} {1}{2}{3}'.format(template_target_folder, server_location, template_source_folder, template_image))
        client.run('chown root {0}{1}'.format(template_target_folder, template_image))

    @staticmethod
    def get_template_location_by_ip(ip):
        if ip.split('.')[0] == '172' and ip.split('.')[1] == '20':
            return TEMPLATE_SERVERS['axs']
        else:
            return TEMPLATE_SERVERS['loch']

    @staticmethod
    def check_template_exists(client):
        if client.dir_exists(template_target_folder):
            client.run('rm -rf {0}'.format(template_target_folder))
            client.run('mkdir {0}'.format(template_target_folder))
        GeneralVDisk.download_template(GeneralVDisk.get_template_location_by_ip(client.ip), client)

    @staticmethod
    def invalidate_attribute(vdisk_name, attributes_to_invalidate):
        vdisk_list = VDiskList.get_vdisk_by_name(vdisk_name)
        if vdisk_list:
            for disk in vdisk_list:
                disk.invalidate_dynamics(attributes_to_invalidate)
