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
Init for System testsuite
"""
from ci.tests.general.general import General
from ci.tests.general.general_storagerouter import GeneralStorageRouter


def setup():
    """
    Setup for System package, will be executed when any test in this package is being executed
    Make necessary changes before being able to run the tests
    :return: None
    """
    print "setup called " + __name__
    settings = None
    if GeneralStorageRouter.get_local_storagerouter().pmachine.hvtype == 'VMWARE':
        settings = {'hypervisor': ['ip', 'username', 'password']}
    General.validate_required_config_settings(settings=settings)
    General.cleanup()


def teardown():
    """
    Teardown for System package, will be executed when all started tests in this package have ended
    Removal actions of possible things left over after the test-run
    :return: None
    """
    pass
