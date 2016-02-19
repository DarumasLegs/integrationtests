# Copyright 2015 iNuron NV
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

"""
Management center testsuite
"""

from ci.tests.general.connection import Connection
from ci.tests.general.general import General
from ci.tests.general.general_mgmtcenter import GeneralManagementCenter
from nose.plugins.skip import SkipTest


class TestMgmtCenter(object):
    """
    Management center testsuite
    """
    api = Connection()
    tests_to_run = General.get_tests_to_run(General.get_test_level())

    ######################
    # SETUP AND TEARDOWN #
    ######################

    @staticmethod
    def setup():
        """
        Make necessary changes before being able to run the tests
        :return: None
        """
        GeneralManagementCenter.create_generic_mgmt_center()

    @staticmethod
    def teardown():
        """
        Removal actions of possible things left over after the test-run
        :return: None
        """
        management_centers = TestMgmtCenter.api.get_components('mgmtcenters')
        for mgmcenter in management_centers:
            GeneralManagementCenter.remove_mgmt_center(mgmcenter['guid'])

    #########
    # TESTS #
    #########

    @staticmethod
    def check_reachability_test():
        """
        Verify the management center is reachable
        """
        General.check_prereqs(testcase_number=1,
                              tests_to_run=TestMgmtCenter.tests_to_run)

        devstack_installed = GeneralManagementCenter.is_devstack_installed()
        if devstack_installed is False:
            raise SkipTest

        management_centers = TestMgmtCenter.api.get_components('mgmtcenters')
        issues_found = ""

        for mgmtcenter in management_centers:
            out, err = General.execute_command("ping {0} -c 1".format(mgmtcenter['ip']))
            if "Destination Host Unreachable" in out:
                issues_found += "Management center {0} with ip {1}\n".format(mgmtcenter['name'], mgmtcenter['ip'])

        assert issues_found == "", "Following management centers could not be reached:\n{0}".format(issues_found)

    @staticmethod
    def management_center_connection_test():
        """
        Verify the management center connectivity
        """
        General.check_prereqs(testcase_number=2,
                              tests_to_run=TestMgmtCenter.tests_to_run)

        devstack_installed = GeneralManagementCenter.is_devstack_installed()
        if devstack_installed is False:
            raise SkipTest

        management_centers = TestMgmtCenter.api.get_components('mgmtcenters')
        issues_found = ""

        for mgmtcenter in management_centers:
            if not GeneralManagementCenter.test_connection(mgmtcenter['guid']):
                issues_found += "Management center {0}\n".format(mgmtcenter['name'])

        assert issues_found == "", "Following management centers failed the connection test:\n{0}".format(issues_found)

    @staticmethod
    def check_configured_management_center_test():
        """
        Verify if the management center has been configured correctly
        """
        General.check_prereqs(testcase_number=3,
                              tests_to_run=TestMgmtCenter.tests_to_run)

        devstack_installed = GeneralManagementCenter.is_devstack_installed()
        if devstack_installed is False:
            raise SkipTest

        management_centers = TestMgmtCenter.api.get_components('mgmtcenters')
        issues_found = ""

        for mgmtcenter in management_centers:
            for physical_machine_guid in mgmtcenter['pmachines_guids']:
                if not GeneralManagementCenter.is_host_configured(physical_machine_guid):
                    issues_found += "Mgmtcenter {0} has an unconfigured pmachine with guid {1}\n".format(mgmtcenter['name'], physical_machine_guid)

        assert issues_found == "", "Following pmachines where not configured with their management center:\n{0}".format(issues_found)

    @staticmethod
    def check_unconfigured_management_center_test():
        """
        Verify if the management center has been un-configured correctly
        """
        General.check_prereqs(testcase_number=4,
                              tests_to_run=TestMgmtCenter.tests_to_run)

        devstack_installed = GeneralManagementCenter.is_devstack_installed()
        if devstack_installed is False:
            raise SkipTest

        management_centers = TestMgmtCenter.api.get_components('mgmtcenters')
        issues_found = ""

        for mgmtcenter in management_centers:
            for physical_machine in TestMgmtCenter.api.get_components('pmachines'):
                GeneralManagementCenter.unconfigure_pmachine_with_mgmtcenter(physical_machine['guid'], mgmtcenter['guid'])
                if GeneralManagementCenter.is_host_configured(physical_machine['guid']):
                    issues_found += "Machine {0} is still configured with {1} management center".format(physical_machine['name'], mgmtcenter['name'])
                GeneralManagementCenter.configure_pmachine_with_mgmtcenter(physical_machine['guid'], mgmtcenter['guid'])

        assert issues_found == "", "Following pmachines where still configured with their management center:\n{0}".format(issues_found)
