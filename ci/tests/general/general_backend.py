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
A general class dedicated to Backend and BackendType logic
"""

from ci.tests.general.connection import Connection
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList


class GeneralBackend(object):
    """
    A general class dedicated to Backend and BackendType logic
    """
    api = Connection()

    @staticmethod
    def get_valid_backendtypes():
        """
        Retrieve a list of supported Backend Types
        :return: List of Backend Type Names
        """
        return [backend_type.code for backend_type in BackendTypeList.get_backend_types()]

    @staticmethod
    def get_backends():
        """
        Retrieve a list of all backend
        :return: Data-object list of Backends
        """
        return BackendList.get_backends()

    @staticmethod
    def get_by_name(name):
        """
        Retrieve a backend based on name
        :param name: Name of the backend
        :return: Backend DAL object
        """
        return BackendList.get_by_name(name=name)

    @staticmethod
    def get_backendtype_by_code(code):
        """
        Retrieve the Backend Type based on its code
        :param code: Code of the Backend Type
        :return: Backend Type DAL object
        """
        if code not in GeneralBackend.get_valid_backendtypes():
            raise ValueError('Unsupported backend type {0} provided'.format(code))
        return BackendTypeList.get_backend_type_by_code(code=code)

    @staticmethod
    def add_backend(backend_name, backend_code):
        """
        Add a new backend
        :param backend_name: Name of the Backend to add
        :param backend_code: Code of the Backend Type to add
        :return: Guid of the new Backend
        """
        backend = GeneralBackend.get_by_name(backend_name)
        if backend is not None:
            return backend

        backend_type = GeneralBackend.get_backendtype_by_code(code=backend_code)
        GeneralBackend.api.add('backends', {'name': backend_name,
                                            'backend_type_guid': backend_type.guid})
        return GeneralBackend.get_by_name(backend_name)
