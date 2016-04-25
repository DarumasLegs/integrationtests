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
Init for vPool testsuite
"""

from ci.tests.general.general import General
from ci.tests.general.general_alba import GeneralAlba


def setup():
    """
    Setup for vPool package, will be executed when any test in this package is being executed
    Make necessary changes before being able to run the tests
    :return: None
    """
    General.validate_required_config_settings(settings={'vpool': ['name', 'type', 'readcache_size', 'writecache_size', 'integrate_mgmt',
                                                                  'storage_ip', 'config_params', 'fragment_cache_on_read', 'fragment_cache_on_write'],
                                                        'backend': ['name']})
    GeneralAlba.prepare_alba_backend()


def teardown():
    """
    Teardown for vPool package, will be executed when all started tests in this package have ended
    Removal actions of possible things left over after the test-run
    :return: None
    """
    alba_backend = GeneralAlba.get_by_name(General.get_config().get('backend', 'name'))
    # if alba_backend is not None:
        # GeneralAlba.remove_disks(alba_backend)
        # GeneralAlba.remove_alba_backend(alba_backend)
