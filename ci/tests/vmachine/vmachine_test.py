# Copyright 2014 iNuron NV
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
from ci import autotests
from ci.tests.general import general
from ci.tests.general.connection import Connection
from ci.tests.general.general import test_config
from ci.tests.general.logHandler import LogHandler
from ci.tests.mgmtcenter import generic as mgmtgeneric
from ci.tests.vpool import generic as vpool_generic
from ci.tests.vmachine.general_vdisk import GeneralVDisk
from ci.tests.vmachine.general_vmachine import GeneralVMachine
from nose.plugins.skip import SkipTest
from ovs.extensions.generic.sshclient import SSHClient

logger = LogHandler.get('vmachines', name='vmachine')
logger.logger.propagate = False

testsToRun = general.get_tests_to_run(autotests.get_test_level())

GRID_IP = test_config.get('main', 'grid_ip')
TEMPLATE_VDISK_NAME = test_config.get('vmachine', 'template_disk_name')
TEMPLATE_VMACHINE_NAME = test_config.get('vmachine', 'template_machine_name')
VPOOL_NAME = test_config.get('vpool', 'vpool_name')

NUMBER_OF_DISKS = 10
TIMEOUT = 70
TIMER_STEP = 5


def setup():
    vpool_generic.add_alba_backend()
    mgmtgeneric.create_generic_mgmt_center()
    vpool_generic.add_generic_vpool()
    client = SSHClient(GRID_IP, username='root')
    GeneralVDisk.check_template_exists(client)


def teardown():
    api = Connection.get_connection()
    template_machine_list = api.get_component_by_name('vmachines', TEMPLATE_VMACHINE_NAME)
    if template_machine_list and len(template_machine_list):
        for template in template_machine_list:
            api.remove('vmachines', template['guid'])
    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        time.sleep(TIMER_STEP)
        vdisk_list = api.get_component_by_name('vdisks', TEMPLATE_VDISK_NAME)
        if vdisk_list is None:
            counter = 0
        else:
            counter -= 1
    logger.info("Cleaning vpool")
    general.api_remove_vpool(VPOOL_NAME)
    vpool_generic.remove_alba_backend()
    logger.info("Cleaning management center")
    management_centers = api.get_components('mgmtcenters')
    for mgmcenter in management_centers:
        mgmtgeneric.remove_mgmt_center(mgmcenter['guid'])


def vmachines_with_fio_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=1,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    assert vpool_list, "No vpool found where one was expected"
    vpool = vpool_list[0]
    storage_driver = api.get_component_with_attribute('storagedrivers', 'guid', vpool['storagedrivers_guids'][0])
    storage_router = api.get_component_with_attribute('storagerouters', 'guid', storage_driver['storagerouter_guid'])
    client = SSHClient(storage_router['ip'], username = 'root')
    for disk_number in range(NUMBER_OF_DISKS):
        disk_name = "disk-{0}".format(disk_number)
        GeneralVDisk.create_raw_vdisk_from_template(vpool['name'], disk_name, client)

    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    vpool = vpool_list[0]
    assert len(vpool['vdisks_guids']) == NUMBER_OF_DISKS, "Only {0} out of {1} VDisks have been created".format(len(vpool['vdisks_guids']), NUMBER_OF_DISKS)

    for vmachine_number in range(NUMBER_OF_DISKS):
        machine_name = "machine-{0}".format(vmachine_number)
        GeneralVMachine.create_vmachine_from_existing_raw_vdisk(machine_name, vpool['name'], "disk-{0}".format(vmachine_number), client, wait=True)

    vmachines = api.get_components('vmachines')
    assert len(vmachines) == NUMBER_OF_DISKS, "Only {0} out of {1} VMachines have been created".format(len(vmachines), NUMBER_OF_DISKS)

    # waiting for 5 minutes of FIO activity on the vmachines
    time.sleep(300)
    vmachines = api.get_components('vmachines')
    for vmachine in vmachines:
        assert vmachine['hypervisor_status'] in ['RUNNING'], "Machine {0} has wrong status on the hypervisor: {1}".format(vmachine['name'], vmachine['hypervisor_status'])

    for vmachine_number in range(NUMBER_OF_DISKS):
        GeneralVMachine.remove_vmachine_by_name("machine-{0}".format(vmachine_number), client, wait=True)

    vmachines = api.get_components('vmachines')
    assert len(vmachines) == 0, "Still some vmachines left on the vpool : {0}".format(vmachines)

    logger.info("Removing vpool vdisks from {0} vpool".format(VPOOL_NAME))
    for disk_number in range(NUMBER_OF_DISKS):
        GeneralVDisk.remove_vdisk("disk-{0}".format(disk_number), client, wait=True)

    vpool = api.get_component_by_name('vpools', VPOOL_NAME)[0]
    assert len(vpool['vdisks_guids']) == 0, "Still some disks left on the vpool: {0}".format(vpool['vdisks_guids'])


def set_vmachine_as_template_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=2,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    assert vpool_list, "No vpool found where one was expected"
    vpool = vpool_list[0]
    storage_driver = api.get_component_with_attribute('storagedrivers', 'guid', vpool['storagedrivers_guids'][0])
    storage_router = api.get_component_with_attribute('storagerouters', 'guid', storage_driver['storagerouter_guid'])
    client = SSHClient(storage_router['ip'], username = 'root')

    GeneralVDisk.create_raw_vdisk(vpool['name'], TEMPLATE_VDISK_NAME, client)
    GeneralVMachine.create_vmachine_from_existing_raw_vdisk(TEMPLATE_VMACHINE_NAME, vpool['name'], TEMPLATE_VDISK_NAME, client, wait=True)

    GeneralVMachine.shut_down_vmachine_by_name(TEMPLATE_VMACHINE_NAME, client, wait=True)

    vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_VMACHINE_NAME)
    assert vmachine_list[0]['hypervisor_status'] == 'TURNEDOFF', \
        "VMachine {0} expected to be in status TURNEDOFF, instead found in status {1} after {2} seconds".format(TEMPLATE_VMACHINE_NAME,
                                                                                                                vmachine_list[0]['hypervisor_status'],
                                                                                                                TIMEOUT)
    GeneralVMachine.set_vmachine_as_template(vmachine_list[0]['guid'])

    vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_VMACHINE_NAME)
    assert vmachine_list[0]['is_vtemplate'], 'Vmachine {0} is not a template'.format(TEMPLATE_VMACHINE_NAME)

    GeneralVDisk.invalidate_attribute(TEMPLATE_VDISK_NAME, ['is_vtemplate'])

    vdisks = api.get_component_by_name('vdisks', TEMPLATE_VDISK_NAME)
    assert vdisks[0]['is_vtemplate'], "VDisk flag is_template didn't change for {0}".format(TEMPLATE_VDISK_NAME)


def create_vmachine_from_template_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=3,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    template_machine_list = api.get_component_by_name('vmachines', TEMPLATE_VMACHINE_NAME)
    if template_machine_list is None or template_machine_list[0]['is_vtemplate'] is False:
        raise SkipTest('Template vmachine not found on environment')

    vmachine_from_template_name = 'fromTemplate'
    GeneralVMachine.create_single_clone(vmachine_from_template_name, TEMPLATE_VMACHINE_NAME)

    vmachine_list = api.get_component_by_name('vmachines', vmachine_from_template_name)
    assert len(vmachine_list), 'Vmachine {0} not found'.format(vmachine_from_template_name)

    GeneralVMachine.remove_vmachine_and_vdisks(vmachine_list[0]['guid'])

    vmachine_list = api.get_component_by_name('vmachines', vmachine_from_template_name)
    assert vmachine_list is None, 'Vmachine {0} still in present in the model after deletion'.format(vmachine_from_template_name)
