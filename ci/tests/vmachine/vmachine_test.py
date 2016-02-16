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
from nose.plugins.skip import SkipTest
from ci import autotests
from ci.tests.general.general import test_config
from ci.tests.general.connection import Connection
from ci.tests.vpool import generic
from ci.tests.mgmtcenter import generic as mgmtgeneric
from ci.tests.general import general
from ci.tests.general.logHandler import LogHandler
from ovs.lib.vmachine import VMachineController
from ovs.dal.lists.vdisklist import VDiskList

logger = LogHandler.get('vmachines', name='vmachine')
logger.logger.propagate = False

testsToRun = general.get_tests_to_run(autotests.get_test_level())

VPOOL_NAME = test_config.get('vpool', 'vpool_name')
TEMPLATE_SERVERS = ['http://sso-qpackages-loch.cloudfounders.com/templates/openvstorage', 'http://172.20.3.8/templates/openvstorage']


template_source_folder = '/fio_debian/'
template_image = 'debian.qcow2'
template_target_folder = '/var/tmp/templates/'

NUMBER_OF_DISKS = 10
GRID_IP = test_config.get('main', 'grid_ip')
TIMEOUT = 70
TIMER_STEP = 5
TEMPLATE_DISK_NAME = "templateDisk"
TEMPLATE_MACHINE_NAME = "templateMachine"


def download_template(server_location):
    logger.info("Getting template from {0}".format(server_location))
    out, err = general.execute_command('wget -P {0} {1}{2}{3}'.format(template_target_folder, server_location, template_source_folder, template_image))
    if err:
        logger.error("Error while downloading template: {0}".format(err))
    out, err = general.execute_command('chown root {0}{1}'.format(template_target_folder, template_image))
    if err:
        logger.error("Error while changing user owner to root for template: {0}".format(err))


def get_template_location_by_ip(ip):
    if ip.split('.')[0] == '172' and ip.split('.')[1] == '20':
        return TEMPLATE_SERVERS[1]
    else:
        return TEMPLATE_SERVERS[0]


def check_template_exists():
    cmd = '[ -d {0} ] && echo "Dir exists" || echo "Dir does not exists"'.format(template_target_folder)
    out, err = general.execute_command(cmd)
    if err:
        logger.error("Error while executing command {1}: {0}".format(err, cmd))
    if 'not' not in out:
        general.execute_command('rm -rf {0}'.format(template_target_folder))
        general.execute_command('mkdir {0}'.format(template_target_folder))
    download_template(get_template_location_by_ip(GRID_IP))


def setup():
    check_template_exists()
    generic.add_alba_backend()
    mgmtgeneric.create_generic_mgmt_center()
    generic.add_generic_vpool()


def teardown():
    api = Connection.get_connection()
    template_machine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
    if template_machine_list and len(template_machine_list):
        VMachineController.delete(template_machine_list[0]['guid'])
    vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
    assert vmachine_list is None, 'Template {0} still in present in the model after deletion'.format(TEMPLATE_MACHINE_NAME)
    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    assert vpool_list, "No vpool found where one was expected"
    logger.info("Cleaning vpool")
    general.api_remove_vpool(VPOOL_NAME)
    generic.remove_alba_backend()
    logger.info("Cleaning management center")
    management_centers = api.get_components('mgmtcenters')
    for mgmcenter in management_centers:
        mgmtgeneric.remove_mgmt_center(mgmcenter['guid'])


def create_raw_vdisk_from_template(template_folder, image_name, vpool_name, disk_name):
    logger.info("Starting RAW disk creation from template")
    out, err = general.execute_command('qemu-img convert -O raw {0}{1} /mnt/{2}/{3}.raw'.format(template_folder, image_name, vpool_name, disk_name))
    if err:
        logger.error("Error while creating raw disk: {0}".format(err))


def create_raw_disk(vpool_name, disk_name):
    logger.info("Starting RAW disk creation")
    out, err = general.execute_command('truncate /mnt/{0}/{1}.raw --size 10000000'.format(vpool_name, disk_name))
    if err:
        logger.error("Error while creating raw disk: {0}".format(err))


def create_machine_from_existing_raw_disk(machine_name, vpool_name, disk_name):
    logger.info("Starting vmachine creation from RAW disk")
    out, err = general.execute_command('virt-install --connect qemu:///system -n {0} -r 512 --disk /mnt/{1}/{2}.raw,'
                                       'device=disk --noautoconsole --graphics vnc,listen=0.0.0.0 --vcpus=1 --network network=default,mac=RANDOM,'
                                       'model=e1000 --import'.format(machine_name, vpool_name, disk_name))
    if err:
        logger.error("Error while creating vmachine: {0}".format(err))


def shut_down_vmachine_by_name(machine_name):
    logger.info("Shutting down vmachine {0}".format(machine_name))
    out, err = general.execute_command('virsh destroy {0}'.format(machine_name))
    if err:
        logger.error("Error while shutting down vmachine {0}: {1}".format(machine_name, err))


def start_vmachine_by_name(machine_name):
    logger.info("Starting vmachine {0}".format(machine_name))
    out, err = general.execute_command('virsh start {0}'.format(machine_name))
    if err:
        logger.error("Error while shutting down vmachine {0}: {1}".format(machine_name, err))


def undefine_machine(vmachine_name):
    out, err = general.execute_command('virsh undefine {0}'.format(vmachine_name))
    if err:
        logger.error("Error while removing vmachine: {0}".format(err))


def remove_machine_by_name(vmachine_name):
    api = Connection.get_connection()
    vms = api.get_component_by_name('vmachines', vmachine_name)
    if vms:
        if vms[0]['hypervisor_status'] == 'RUNNING':
            shut_down_vmachine_by_name(vmachine_name)
    vms = api.get_component_by_name('vmachines', vmachine_name)
    if vms:
        if vms[0]['hypervisor_status'] == 'TURNEDOFF':
            undefine_machine(vmachine_name)


def vms_with_fio_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=1,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    assert len(vpool_list), "No vpool found where one was expected"
    vpool = vpool_list[0]
    for disk_number in range(NUMBER_OF_DISKS):
        disk_name = "disk-{0}".format(disk_number)
        create_raw_vdisk_from_template(template_target_folder, template_image, vpool['name'], disk_name)

    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    vpool = vpool_list[0]
    assert len(vpool['vdisks_guids']) == NUMBER_OF_DISKS, "Only {0} out of {1} VDisks have been created".format(len(vpool['vdisks_guids']), NUMBER_OF_DISKS)

    for vm_number in range(NUMBER_OF_DISKS):
        machine_name = "machine-{0}".format(vm_number)
        create_machine_from_existing_raw_disk(machine_name, vpool['name'], "disk-{0}".format(vm_number))

    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        vms = api.get_components('vmachines')
        if len(vms) == NUMBER_OF_DISKS:
            counter = 0
        else:
            counter -= 1
            time.sleep(TIMER_STEP)
    vms = api.get_components('vmachines')
    assert len(vms) == NUMBER_OF_DISKS, "Only {0} out of {1} VMachines have been created after {2} seconds".format(len(vms), NUMBER_OF_DISKS, TIMEOUT)

    # waiting for 5 minutes of FIO activity on the vmachines
    time.sleep(300)
    vms = api.get_components('vmachines')
    for vm in vms:
        assert vm['hypervisor_status'] in ['RUNNING'], "Machine {0} has wrong status on the hypervisor: {1}".format(vm['name'], vm['hypervisor_status'])

    for vm_number in range(NUMBER_OF_DISKS):
        remove_machine_by_name("machine-{0}".format(vm_number))

    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        vms = api.get_components('vmachines')
        if len(vms):
            counter -= 1
            time.sleep(TIMER_STEP)
        else:
            counter = 0
    vms = api.get_components('vmachines')
    assert len(vms) == 0, "Still some machines left on the vpool after waiting for {0} seconds: {1}".format(TIMEOUT, vms)

    logger.info("Removing vpool vdisks from {0} vpool".format(VPOOL_NAME))
    out, err = general.execute_command("rm -rf /mnt/{0}/*.raw".format(VPOOL_NAME))
    if err:
        logger.error("Error while removing vdisks: {0}".format(err))

    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        vpool = api.get_component_by_name('vpools', VPOOL_NAME)[0]
        if len(vpool['vdisks_guids']):
            counter -= 1
            time.sleep(TIMER_STEP)
        else:
            counter = 0
    vpool = api.get_component_by_name('vpools', VPOOL_NAME)[0]
    assert len(vpool['vdisks_guids']) == 0, "Still some disks left on the vpool after waiting {0} seconds: {1}".format(TIMEOUT, vpool['vdisks_guids'])


def set_vm_as_template_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=2,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    vpool_list = api.get_component_by_name('vpools', VPOOL_NAME)
    assert len(vpool_list), "No vpool found where one was expected"
    vpool = vpool_list[0]

    create_raw_disk(vpool['name'], TEMPLATE_DISK_NAME)
    create_machine_from_existing_raw_disk(TEMPLATE_MACHINE_NAME, vpool['name'], TEMPLATE_DISK_NAME)

    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
        if vmachine_list:
            counter = 0
        else:
            counter -= 1
            time.sleep(TIMER_STEP)

    shut_down_vmachine_by_name(TEMPLATE_MACHINE_NAME)

    counter = TIMEOUT / TIMER_STEP
    while counter > 0:
        vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
        if vmachine_list[0]['hypervisor_status'] == 'TURNEDOFF':
            counter = 0
        else:
            counter -= 1
            time.sleep(TIMER_STEP)
    vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
    assert vmachine_list[0]['hypervisor_status'] == 'TURNEDOFF', \
        "VMachine {0} expected to be in status TURNEDOFF, instead found in status {1} after {2} seconds".format(TEMPLATE_MACHINE_NAME,
                                                                                                                vmachine_list[0]['hypervisor_status'],
                                                                                                                TIMEOUT)
    VMachineController.set_as_template(vmachine_list[0]['guid'])

    vmachine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
    assert vmachine_list[0]['is_vtemplate'], 'Vmachine {0} is not a template'.format(TEMPLATE_MACHINE_NAME)

    vdisk_list = VDiskList.get_vdisk_by_name(TEMPLATE_DISK_NAME)
    if vdisk_list and len(vdisk_list):
        vdisk_list[0].invalidate_dynamics(['is_vtemplate'])

    vdisks = api.get_component_by_name('vdisks', TEMPLATE_DISK_NAME)
    assert vdisks[0]['is_vtemplate'], "VDisk {0} expected to be changed to template after {1} seconds".format(TEMPLATE_DISK_NAME, TIMEOUT)


def create_vm_from_template_test():
    """
    {0}
    """.format(general.get_function_name())

    general.check_prereqs(testcase_number=3,
                          tests_to_run=testsToRun)

    api = Connection.get_connection()
    template_machine_list = api.get_component_by_name('vmachines', TEMPLATE_MACHINE_NAME)
    if template_machine_list is None or len(template_machine_list) == 0:
        raise SkipTest()

    assert template_machine_list[0]['is_vtemplate'], 'Vmachine {0} is not a template'.format(TEMPLATE_MACHINE_NAME)

    machine_from_template_name = 'fromTemplate'
    pms = api.get_components('pmachines')
    for pm in pms:
        if pm['ip'] == GRID_IP:
            VMachineController.create_from_template(machine_from_template_name, template_machine_list[0]['guid'], pm['guid'])

    vmachine_list = api.get_component_by_name('vmachines', machine_from_template_name)
    assert len(vmachine_list), 'Vmachine {0} not found'.format(machine_from_template_name)

    VMachineController.delete(vmachine_list[0]['guid'])

    vmachine_list = api.get_component_by_name('vmachines', machine_from_template_name)
    assert vmachine_list is None, 'Vmachine {0} still in present in the model after deletion'.format(machine_from_template_name)
