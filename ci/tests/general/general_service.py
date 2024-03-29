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
A general class dedicated to Service and ServiceType logic
"""

import os
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.services.systemd import Systemd
from ovs.extensions.services.upstart import Upstart


class GeneralService(object):
    """
    A general class dedicated to Service and ServiceType logic
    """
    @staticmethod
    def get_services_by_name(name):
        """
        Retrieve all services for a certain type
        :param name: Name of the service type
        :return: Data-object list of Services
        """
        service_type_names = [service_type.name for service_type in GeneralService.get_service_types()]
        if name not in service_type_names:
            raise ValueError('Invalid Service Type name specified. Please choose from: {0}'.format(', '.format(service_type_names)))
        return ServiceTypeList.get_by_name(name).services

    @staticmethod
    def get_service_types():
        """
        Retrieve all service types
        :return: Data-object list of ServiceTypes
        """
        return ServiceTypeList.get_servicetypes()

    @staticmethod
    def get_all_service_templates():
        """
        Retrieve all templates for each defined service
        :return: List of service names
        """
        _ = ServiceManager.is_enabled  # Invoking the __getattr__ function of the MetaClass
        if ServiceManager.ImplementationClass == Systemd:
            services_folder = '/opt/OpenvStorage/config/templates/systemd/'
        elif ServiceManager.ImplementationClass == Upstart:
            services_folder = '/opt/OpenvStorage/config/templates/upstart/'
        else:
            raise RuntimeError('Unsupported ServiceManager class found: {0}'.format(ServiceManager.ImplementationClass))

        if not os.path.isdir(services_folder):
            raise RuntimeError('Directory {0} could not be found on this system'.format(services_folder))

        if ServiceManager.ImplementationClass == Systemd:
            return [service_name.replace('.service', '') for service_name in os.listdir(services_folder)]
        return [service_name.replace('.conf', '') for service_name in os.listdir(services_folder)]

    @staticmethod
    def has_service(name, client):
        """
        Validate if the node has the service configured
        :param name: Name of the service
        :param client: SSHClient object
        :return: True if service is configured
        """
        return ServiceManager.has_service(name, client)

    @staticmethod
    def get_service_status(name, client):
        """
        Check the status of the service
        :param name: Name of the service
        :param client: SSHClient object
        :return: True if service is running
        """
        return ServiceManager.get_service_status(name, client)

    @staticmethod
    def start_service(name, client):
        """
        Start a service
        :param name: Name of the service
        :param client: SSHClient object
        :return: None
        """
        ServiceManager.start_service(name, client)

    @staticmethod
    def stop_service(name, client):
        """
        Stop a service
        :param name: Name of the service
        :param client: SSHClient object
        :return: None
        """
        ServiceManager.stop_service(name, client)

    @staticmethod
    def get_service_pid(name, client):
        """
        Retrieve the PID of a service
        :param name: Name of the service to retrieve the PID for
        :param client: SSHClient object
        :return: PID
        """
        return ServiceManager.get_service_pid(name, client)

    @staticmethod
    def kill_service(name, client):
        """
        Kill a service
        :param name: Name of the service to kill
        :param client: SSHClient object
        :return: None
        """
        pid = GeneralService.get_service_pid(name, client)
        if pid != -1:
            client.run('kill -9 {0}'.format(pid))
