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
from ci.tests.general.logHandler import LogHandler
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.lib.vmachine import VMachineController

logger = LogHandler.get('vmachines', name='vmachine')
logger.logger.propagate = False

TIMEOUT = 70
TIMER_STEP = 5
api = Connection.get_connection()


class GeneralVMachine(object):

    # @TODO: add functionality for VMWARE as well
    @staticmethod
    def create_vmachine_from_existing_raw_vdisk(vmachine_name, vpool_name, vdisk_name, client, wait=True):
        logger.info("Starting vmachine creation from RAW disk")
        client.run('virt-install --connect qemu:///system -n {0} -r 512 --disk /mnt/{1}/{2}.raw,'
                   'device=disk --noautoconsole --graphics vnc,listen=0.0.0.0 --vcpus=1 --network network=default,mac=RANDOM,'
                   'model=e1000 --import'.format(vmachine_name, vpool_name, vdisk_name))
        if wait:
            counter = TIMEOUT / TIMER_STEP
            while counter > 0:
                vmachine_list = api.get_component_by_name('vmachines', vmachine_name)
                if vmachine_list:
                    counter = 0
                else:
                    counter -= 1
                    time.sleep(TIMER_STEP)

    @staticmethod
    def shut_down_vmachine_by_name(vmachine_name, client, wait=True):
        logger.info("Shutting down vmachine {0}".format(vmachine_name))
        client.run('virsh destroy {0}'.format(vmachine_name))
        vmachine_list = VMachineList.get_vmachine_by_name(vmachine_name)
        if vmachine_list:
            vmachine_list[0].invalidate_dynamics(['hypervisor_status'])
        if wait:
            counter = TIMEOUT / TIMER_STEP
            while counter > 0:
                vmachine_list = VMachineList.get_vmachine_by_name(vmachine_name)
                if vmachine_list[0].hypervisor_status == 'TURNEDOFF':
                    counter = 0
                else:
                    counter -= 1
                    time.sleep(TIMER_STEP)

    @staticmethod
    def start_vmachine_by_name(vmachine_name, client, wait=True):
        logger.info("Starting vmachine {0}".format(vmachine_name))
        client.run('virsh start {0}'.format(vmachine_name))
        vmachine_list = VMachineList.get_vmachine_by_name(vmachine_name)
        if vmachine_list:
            vmachine_list[0].invalidate_dynamics(['hypervisor_status'])

        if wait:
            counter = TIMEOUT / TIMER_STEP
            while counter > 0:
                vmachine_list = VMachineList.get_vmachine_by_name(vmachine_name)
                if vmachine_list[0].hypervisor_status == 'TURNEDOFF':
                    counter = 0
                else:
                    counter -= 1
                    time.sleep(TIMER_STEP)

    @staticmethod
    def undefine_vmachine(vmachine_name, client):
        client.run('virsh undefine {0}'.format(vmachine_name))

    @staticmethod
    def remove_vmachine_by_name(vmachine_name, client, wait=True):
        vms = api.get_component_by_name('vmachines', vmachine_name)
        if vms:
            if vms[0]['hypervisor_status'] == 'RUNNING':
                GeneralVMachine.shut_down_vmachine_by_name(vmachine_name, client)
        vms = api.get_component_by_name('vmachines', vmachine_name)
        if vms:
            if vms[0]['hypervisor_status'] == 'TURNEDOFF':
                GeneralVMachine.undefine_vmachine(vmachine_name, client)
        if wait:
            counter = TIMEOUT / TIMER_STEP
            while counter > 0:
                vmachine_list = api.get_component_by_name('vmachines', vmachine_name)
                if vmachine_list:
                    counter -= 1
                    time.sleep(TIMER_STEP)
                else:
                    counter = 0

    @staticmethod
    def set_vmachine_as_template(vmachine_guid):
        VMachineController.set_as_template(vmachine_guid)

    @staticmethod
    def remove_vmachine_and_vdisks(vmachine_guid):
        VMachineController.delete(vmachine_guid)

    @staticmethod
    def create_single_clone(clone_name, template_name):
        template_machine_list = api.get_component_by_name('vmachines', template_name)
        VMachineController.create_from_template(clone_name, template_machine_list[0]['guid'], template_machine_list[0]['pmachine_guid'])
