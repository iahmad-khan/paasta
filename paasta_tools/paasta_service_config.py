# Copyright 2015-2016 Yelp Inc.
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
import logging

from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_service_configuration

from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_deployments_json


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class PaastaServiceConfig():

    def __init__(self, service, soa_dir=DEFAULT_SOA_DIR, load_deployments=True):
        self._service = service
        self._soa_dir = soa_dir
        self._load_deployments = load_deployments
        self._clusters = None
        self._general_config = None
        self._deployments_json = None
        self._framework_configs = {}
        self._deployments_json = None

    @property
    def clusters(self):
        """Yield cluster names for the service"""
        if self._clusters is None:
            self._clusters = list_clusters(service=self._service, soa_dir=self._soa_dir)
        for cluster in self._clusters:
            yield cluster

    def instances(self, cluster, instance_type):
        """Yield instance names as a string.

        :param cluster: The cluster name
        :param instance_type: One of paasta_tools.utils.INSTANCE_TYPES
        """
        if (cluster, instance_type) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type)
        for instance in self._framework_configs.get((cluster, instance_type), []):
            yield instance

    def instance_configs(self, cluster, instance_type):
        if (cluster, instance_type) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type)
        for instance, config in self._framework_configs.get((cluster, instance_type), []).items():
            yield self._create_marathon_service_config(cluster, instance, config)

    def _framework_config_filename(self, cluster, instance_type):
        return "%s-%s" % (instance_type, cluster)

    def _refresh_framework_config(self, cluster, instance_type):
        conf_name = self._framework_config_filename(cluster, instance_type)
        log.info("Reading configuration file: %s.yaml", conf_name)
        instances = read_extra_service_information(
            service_name=self._service,
            extra_info=conf_name,
            soa_dir=self._soa_dir,
        )
        self._framework_configs[(cluster, instance_type)] = instances

    def _get_branch_dict(self, cluster, instance, config):
        if self._load_deployments:
            if self._deployments_json is None:
                self._deployments_json = load_deployments_json(self._service, soa_dir=self._soa_dir)
            branch = config.get('branch', get_paasta_branch(cluster, instance))
            return self._deployments_json.get_branch_dict(self._service, branch)
        else:
            return {}

    def _create_marathon_service_config(self, cluster, instance, config):
        """Create a service instance's configuration for marathon.

        :param cluster: The cluster to read the configuration for
        :param instance: The instance of the service to retrieve
        :param config:
        :returns: An instance of MarathonServiceConfig
        """
        if self._general_config is None:
            self._general_config = read_service_configuration(
                service_name=self._service,
                soa_dir=self._soa_dir,
            )

        merged_config = deep_merge_dictionaries(
            overrides=config,
            defaults=self._general_config,
        )

        branch_dict = self._get_branch_dict(cluster, instance, merged_config)

        return MarathonServiceConfig(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=branch_dict,
            soa_dir=self._soa_dir,
        )
