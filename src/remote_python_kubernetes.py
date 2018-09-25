import sys
import os
import json
import logging
import time
import requests
import ruxit.api.exceptions
from ruxit.api.base_plugin import RemoteBasePlugin
from requests.exceptions import ReadTimeout
from urllib.parse import urlparse
from collections import defaultdict, namedtuple

logger = logging.getLogger(__name__)


#class RemoteKubernetesPlugin():
class RemoteKubernetesPlugin(RemoteBasePlugin):
    def initialize(self, **kwargs):
        try:
            self.args = {}

            self.args['id'] = kwargs['config']['id']
            self.args['url'] = kwargs['config']['url']
            self.args['token'] = kwargs['config']['token']
            self.args['debug'] = kwargs['config']['debug']

            if 'dev' in kwargs['config']:
                self.args['dev'] = "true"
            else:
                self.args['dev'] = "false"

            self.args['metrics'] = self.initialize_metrics(kwargs['json_config']['metrics'])

            self.log("[remotekubernetesplugin.initialize] ID: " + str(self.args['id']), "info")
            self.log("[remotekubernetesplugin.initialize] URL: " + str(self.args['url']), "info")
            self.log("[remotekubernetesplugin.initialize] TOKEN: " + str(self.args['token']), "info")
            self.log("[remotekubernetesplugin.initialize] DEBUG: " + str(self.args['debug']), "info")
            self.log("[remotekubernetesplugin.initialize] METRICS: " + str(self.args['metrics']), "info")
            self.log("[remotekubernetesplugin.initialize] DEV: " + str(self.args['dev']), "info")

            return
        except Exception as exc:
            self.log("[remotekubernetesplugin.initialize] EXCEPTION: " + str(exc), "info")

            return

    def initialize_metrics(self, json_config_metrics):
        try:
            results = []

            for metric in json_config_metrics:
                result = {'entity': 'CUSTOM_DEVICE', 'key': metric['timeseries']['key'],
                          'dimensions': metric['timeseries']['dimensions'], 'type': metric['source']['type'],
                          'relative': metric['source']['relative']}

                results.append(result)

            return results
        except Exception as exc:
            self.log("[remotekubernetesplugin.initialize_metrics] EXCEPTION: " + str(exc), "info")

            return

    def query(self, **kwargs):
        try:
            self.log("[remotekubernetesplugin.query]", "info")

            start_time = time.time()

            data = {'cluster': self.query_cluster(),
                    'nodes': self.query_nodes(),
                    'services': self.query_services(),
                    'pods': self.query_pods()}

            self.report_topology(data)

            end_time = time.time()

            print("--- STATISTICS ---")
            print("--- EXECUTION TIME: %s SECONDS ---" % (end_time - start_time))

            return
        except Exception as exc:
            self.log("[remotekubernetesplugin.query] EXCEPTION: " + str(exc), "info")

            return

    def query_cluster(self):
        try:
            self.log("[remotekubernetesplugin.query_cluster]", "info")

            cluster = {}

            cluster['cluster_id'] = str(self.args['id'])
            cluster['cluster_url'] = str(self.args['url'])
            cluster['cluster_name'] = str(self.args['id']) + " (" + str(self.args['url']) + ")"

            return cluster
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_cluster] EXCEPTION: " + str(exc), "info")

            return

    def query_nodes(self):
        try:
            self.log("[remotekubernetesplugin.query_nodes]", "info")

            nodes = []

            url = str(self.args['url']) + "/api/v1/nodes"
            content = self.query_url(url)
            json_data = json.loads(content)

            for item in json_data['items']:
                self_link = item['metadata']['selfLink']
                nodes.append(self.query_node(self_link))

            return nodes
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_nodes] EXCEPTION: " + str(exc), "info")

            return

    def query_node(self, self_link):
        try:
            self.log("[remotekubernetesplugin.query_node]", "info")

            node = {}

            url = str(self.args['url']) + str(self_link)
            content = self.query_url(url)
            json_data = json.loads(content)

            node['node_id'] = json_data['metadata']['uid']
            node['node_name'] = json_data['metadata']['name']

            for address in json_data['status']['addresses']:
                node['node_external_ip'] = address['address']

                if address['type'] == "ExternalIP":
                    break

            try:
                node['node_role'] = json_data['metadata']['labels']['kubernetes.io/role']
            except Exception as exc:
                node['node_role'] = "node"

            try:
                node['node_instance_type'] = json_data['metadata']['labels']['beta.kubernetes.io/instance-type']
            except Exception as exc:
                node['node_instance_type'] = ""

            try:
                node['node_hostname'] = json_data['metadata']['labels']['kubernetes.io/hostname']
            except Exception as exc:
                node['node_hostname'] = ""

            try:
                node['node_creation_timestamp'] = json_data['metadata']['creationTimestamp']
            except Exception as exc:
                node['node_creation_timestamp'] = ""

            try:
                node['node_info_machine_id'] = json_data['status']['nodeInfo']['machineID']
            except Exception as exc:
                node['node_info_machine_id'] = ""

            try:
                node['node_info_system_uuid'] = json_data['status']['nodeInfo']['systemUUID']
            except Exception as exc:
                node['node_info_system_uuid'] = ""

            try:
                node['node_info_boot_id'] = json_data['status']['nodeInfo']['bootID']
            except Exception as exc:
                node['node_info_boot_id'] = ""

            try:
                node['node_info_kernel_version'] = json_data['status']['nodeInfo']['kernelVersion']
            except Exception as exc:
                node['node_info_kernel_version'] = ""

            try:
                node['node_info_os_image'] = json_data['status']['nodeInfo']['osImage']
            except Exception as exc:
                node['node_info_os_image'] = ""

            try:
                node['node_info_container_runtime_version'] = json_data['status']['nodeInfo']['containerRuntimeVersion']
            except Exception as exc:
                node['node_info_container_runtime_version'] = ""

            try:
                node['node_info_kubelet_version'] = json_data['status']['nodeInfo']['kubeletVersion']
            except Exception as exc:
                node['node_info_kubelet_version'] = ""

            try:
                node['node_info_kube_proxy_version'] = json_data['status']['nodeInfo']['kubeProxyVersion']
            except Exception as exc:
                node['node_info_kube_proxy_version'] = ""

            try:
                node['node_info_operating_system'] = json_data['status']['nodeInfo']['operatingSystem']
            except Exception as exc:
                node['node_info_operating_system'] = ""

            try:
                node['node_info_architecture'] = json_data['status']['nodeInfo']['architecture']
            except Exception as exc:
                node['node_info_architecture'] = ""

            return node
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_node] EXCEPTION: " + str(exc), "info")

            return

    def query_services(self):
        try:
            self.log("[remotekubernetesplugin.query_services]", "info")

            services = []

            url = str(self.args['url']) + "/api/v1/services"
            content = self.query_url(url)
            json_data = json.loads(content)

            for item in json_data['items']:
                self_link = item['metadata']['selfLink']
                services.append(self.query_service(self_link))

            return services
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_services] EXCEPTION: " + str(exc), "info")

            return

    def query_service(self, self_link):
        try:
            self.log("[remotekubernetesplugin.query_service]", "info")

            service = {}

            url = str(self.args['url']) + str(self_link)
            content = self.query_url(url)
            json_data = json.loads(content)

            service['service_id'] = json_data['metadata']['uid']
            service['service_self_link'] = json_data['metadata']['selfLink']

            return service
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_service] EXCEPTION: " + str(exc), "info")

            return

    def query_pods(self):
        try:
            self.log("[remotekubernetesplugin.query_pods]", "info")

            pods = []

            url = str(self.args['url']) + "/api/v1/pods"
            content = self.query_url(url)
            json_data = json.loads(content)

            for item in json_data['items']:
                self_link = item['metadata']['selfLink']
                pods.append(self.query_pod(self_link))

            return pods
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_pods] EXCEPTION: " + str(exc), "info")

            return

    def query_pod(self, self_link):
        try:
            self.log("[remotekubernetesplugin.query_pod]", "info")

            pod = {}

            url = str(self.args['url']) + str(self_link)
            content = self.query_url(url)
            json_data = json.loads(content)

            pod['pod_id'] = json_data['metadata']['uid']
            pod['pod_name'] = json_data['metadata']['name']
            pod['pod_self_link'] = json_data['metadata']['selfLink']

            try:
                pod['pod_node_name'] = json_data['spec']['nodeName']
            except Exception as exc:
                pod['pod_node_name'] = None

            pod['pod_metrics_endpoint'] = str(self.args['url']) + json_data['metadata']['selfLink'] + str('/proxy/metrics')

            pod['pod_metrics'] = self.query_metrics(pod['pod_metrics_endpoint'])

            return pod
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_pod] EXCEPTION: " + str(exc), "info")

            return

    def query_metrics(self, metrics_endpoint):
        try:
            self.log("[remotekubernetesplugin.query_metrics]", "info")

            results = []

            content = self.query_url(metrics_endpoint)
            lines = content.split('\n')

            if not lines[0].startswith('#'):
                return []

            parsed_metrics = self.parse(lines)

            for parsed_metric in parsed_metrics:
                for metric in self.args['metrics']:
                    if parsed_metric['key'] == metric['key']:
                        result = {}

                        result['entity'] = 'CUSTOM_DEVICE'
                        result['key'] = metric['key']
                        result['type'] = metric['type']
                        result['relative'] = metric['relative']

                        result['value'] = parsed_metric['value']

                        dimensions = {}
                        for dimension in parsed_metric['dimensions']:
                            dimensions[dimension['key']] = dimension['value']

                        result['dimensions'] = dimensions

                        results.append(result)
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_metrics] EXCEPTION: " + str(exc), "info")
            results = []

        return results

    def query_url(self, url):
        try:
            self.log("[remotekubernetesplugin.query_url]", "info")
            self.log("[remotekubernetesplugin.query_url] URL: " + url, "info")

            content = None

            max_retries = 2
            retries = 0
            while retries < max_retries:
                retries = retries + 1
                try:
                    r = requests.get(url, verify=False, headers={"Authorization": "Bearer " + self.args['token']}, timeout=2)
                    content = r.content.decode('UTF-8')

                    self.log("[remotekubernetesplugin.query_url] RESPONSE: " + str(content), "info")

                    return content
                except Exception as exc:
                    self.log("[remotekubernetesplugin.query_url] EXCEPTION: " + str(exc), "info")
                    continue

            return content
        except Exception as exc:
            self.log("[remotekubernetesplugin.query_url] EXCEPTION: " + str(exc), "info")

            return

    def parse(self, lines):
        try:
            self.log("[remotekubernetesplugin.parse]", "info")

            metrics = []

            for line in lines:
                if line.startswith("#") or line == '':
                    continue

                metric = {'key': self.parse_key(line),
                          'dimensions': self.parse_dimensions(line),
                          'value': self.parse_value(line)}

                metrics.append(metric)

            return metrics
        except Exception as exc:
            self.log("[remotekubernetesplugin.parse] EXCEPTION: " + str(exc), "info")

            return

    def parse_key(self, line):
        try:
            self.log("[remotekubernetesplugin.parse_key]", "info")

            return str(line.split(' ')[0].split('{')[0])
        except Exception as exc:
            self.log("[remotekubernetesplugin.parse_key] EXCEPTION: " + str(exc), "info")

            return

    def parse_value(self, line):
        try:
            self.log("[remotekubernetesplugin.parse_value]", "info")

            return str(line.split(' ')[1])
        except Exception as exc:
            self.log("[remotekubernetesplugin.parse_value] EXCEPTION: " + str(exc), "info")

            return

    def parse_dimensions(self, line):
        try:
            self.log("[remotekubernetesplugin.parse_dimensions]", "info")

            dimensions = []

            if len(line.split(' ')[0].split('{')) >= 2:
                dims_temp = str(str(line.split(' ')[0].split('{')[1])).replace('{', '').replace('}', '').replace('"', '').split(',')

                for dim_temp in dims_temp:
                    if len(dim_temp.split('=')) >= 2:
                        dim_key = dim_temp.split('=')[0]
                        dim_value = dim_temp.split('=')[1]

                        dimension = {}
                        dimension['key'] = dim_key
                        dimension['value'] = dim_value

                        dimensions.append(dimension)

            return dimensions
        except Exception as exc:
            self.log("[remotekubernetesplugin.parse_dimensions] EXCEPTION: " + str(exc), "info")

            return

    def report_topology(self, data):
        try:
            self.log("[remotekubernetesplugin.report_topology]", "info")

            group = self.report_topology_group(data['cluster']['cluster_id'], data['cluster']['cluster_name'])

            reported_group_metrics = []

            for node in data["nodes"]:
                element = self.report_topology_element(group, node['node_id'], node['node_name'] + str(" (") + str(node['node_role']) + str(")"), node['node_external_ip'])

                if self.args['dev'] == "false":
                    self.log("[remotekubernetesplugin.report_topology] Reporting properties", "info")

                    element.report_property('node_instance_type', node['node_instance_type'])
                    element.report_property('node_hostname', node['node_hostname'])
                    element.report_property('node_creation_timestamp', node['node_creation_timestamp'])
                    element.report_property('node_info_machine_id', node['node_info_machine_id'])
                    element.report_property('node_info_system_uuid', node['node_info_system_uuid'])
                    element.report_property('node_info_boot_id', node['node_info_boot_id'])
                    element.report_property('node_info_kernel_version', node['node_info_kernel_version'])
                    element.report_property('node_info_os_image', node['node_info_os_image'])
                    element.report_property('node_info_container_runtime_version', node['node_info_container_runtime_version'])
                    element.report_property('node_info_kubelet_version', node['node_info_kubelet_version'])
                    element.report_property('node_info_kube_proxy_version', node['node_info_kube_proxy_version'])
                    element.report_property('node_info_operating_system', node['node_info_operating_system'])
                    element.report_property('node_info_architecture', node['node_info_architecture'])

                if data["pods"] is not None:
                    for pod in data["pods"]:
                        reported_element_metrics = []

                        if pod['pod_metrics'] is not None:
                            for metric in pod['pod_metrics']:
                                if self.is_reportable(data, metric, node['node_name'], pod['pod_name']):
                                    self.report_topology_metric(element, metric)
                                    reported_element_metrics.append(metric)
                                    reported_group_metrics.append(metric)

                            self.report_topology_custom_element_metrics(element, reported_element_metrics)

            self.report_topology_custom_group_metrics(group, reported_group_metrics)
        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology] EXCEPTION: " + str(exc), "info")

            return

    def report_topology_group(self, id, name):
        try:
            self.log("[remotekubernetesplugin.report_topology_group]", "info")
            self.log("[remotekubernetesplugin.report_topology_group] Create topology group", "info")
            self.log("[remotekubernetesplugin.report_topology_group] Create topology group: ID=" + str(id), "info")
            self.log("[remotekubernetesplugin.report_topology_group] Create topology group: NAME=" + str(name), "info")

            if self.args['dev'] == "false":
                topology_group = self.topology_builder.create_group(id, name)

                return topology_group
        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology_group] EXCEPTION: " + str(exc), "info")

            return

    def report_topology_element(self, group, id, name, external_ip):
        try:
            self.log("[remotekubernetesplugin.report_topology_element]", "info")
            self.log("[remotekubernetesplugin.report_topology_element] Create topology +-- element", "info")
            self.log("[remotekubernetesplugin.report_topology_element] Create topology +-- element: GROUP=" + str(group), "info")
            self.log("[remotekubernetesplugin.report_topology_element] Create topology +-- element: ID=" + str(id), "info")
            self.log("[remotekubernetesplugin.report_topology_element] Create topology +-- element: NAME=" + str(name), "info")
            self.log("[remotekubernetesplugin.report_topology_element] Create topology +-- element: EXTERNAL_IP=" + str(external_ip), "info")

            if self.args['dev'] == "false":
                topology_element = group.create_element(id, name)

                # Experimental feature (add endpoint for problem correlation)
                # topology_element.add_endpoint(external_ip, 80)

                return topology_element
        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology_element] EXCEPTION: " + str(exc), "info")

            return

    def report_topology_metrics(self, element, metrics):
        try:
            self.log("[remotekubernetesplugin.report_topology_metrics]", "info")

            for metric in metrics:
                self.report_topology_metric(element, metric)

        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology_metrics] EXCEPTION: " + str(exc), "info")

            exit(-1)

    def report_topology_metric(self, element, metric):
        try:
            self.log("[remotekubernetesplugin.report_topology_metric]", "info")
            self.log("[remotekubernetesplugin.report_topology_metric] Create topology +---- metric: " + " " + str(metric), "info")

            if self.args['dev'] == "false":
                if metric['relative']:
                    element.relative(key=metric['key'], value=metric['value'], dimensions=metric['dimensions'])
                else:
                    element.absolute(key=metric['key'], value=metric['value'], dimensions=metric['dimensions'])
        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology_metric] EXCEPTION: " + str(exc), "info")

            return

    def is_reportable(self, data, metric, reporting_node_name, reporting_pod_name):
        try:
            self.log("[remotekubernetesplugin.is_reportable]", "info")

            if len(metric['dimensions']) > 0:
                for dimension in metric['dimensions']:
                    if dimension == 'pod':
                        pod_name = metric['dimensions'][dimension]
                        for pod in data['pods']:
                            if pod['pod_name'] == pod_name:
                                if pod['pod_node_name'] == reporting_node_name:
                                    return True

                    if dimension == 'deployment':
                        return True
            else:
                for pod in data['pods']:
                    if pod['pod_name'] == reporting_pod_name:
                        if pod['pod_node_name'] == reporting_node_name:
                            return True

            return False
        except Exception as exc:
            self.log("[remotekubernetesplugin.is_reportable] EXCEPTION: " + str(exc), "info")

            return

    def exists_metric(self, metrics, key):
        try:
            self.log("[remotekubernetesplugin.exists_metric]", "info")

            for metric in metrics:
                if metric['key'] == key:
                    return True

            return False
        except Exception as exc:
            self.log("[remotekubernetesplugin.exists_metric] EXCEPTION: " + str(exc), "info")

            return

    def report_topology_custom_element_metrics(self, element, metrics):
        try:
            self.log("[remotekubernetesplugin.report_topology_custom_element_metrics]", "info")

            if self.exists_metric(metrics, "kube_pod_container_status_ready"):
                custom_pods_ready = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_ready", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}
                custom_pods_not_ready = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_not_ready", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}
                custom_pods_total = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_total", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

                for metric in metrics:
                    if metric['key'] == "kube_pod_container_status_ready":
                        custom_pods_total['value'] = int(custom_pods_total['value']) + 1
                        if int(metric['value']) == 1:
                            custom_pods_ready['value'] = int(custom_pods_ready['value']) + 1
                        else:
                            custom_pods_not_ready['value'] = int(custom_pods_not_ready['value']) + 1

                self.report_topology_metric(element, custom_pods_ready)
                self.report_topology_metric(element, custom_pods_not_ready)
                self.report_topology_metric(element, custom_pods_total)

            if self.exists_metric(metrics, "kube_pod_container_status_ready"):
                custom_deployments_available = {'entity': 'CUSTOM_DEVICE', 'key': "custom_deployments_available", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

                for metric in metrics:
                    if metric['key'] == "kube_deployment_status_replicas_available":
                        if int(metric['value']) == 1:
                            custom_deployments_available['value'] = int(custom_deployments_available['value']) + 1

                self.report_topology_metric(element, custom_deployments_available)

            if self.exists_metric(metrics, "kube_pod_container_status_ready"):
                custom_deployments_unavailable = {'entity': 'CUSTOM_DEVICE', 'key': "custom_deployments_unavailable", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

                for metric in metrics:
                    if metric['key'] == "kube_deployment_status_replicas_unavailable":
                        if int(metric['value']) == 1:
                            custom_deployments_unavailable['value'] = int(custom_deployments_unavailable['value']) + 1

                self.report_topology_metric(element, custom_deployments_unavailable)
        except Exception as exc:
            self.log("[remotekubernetesplugin.report_topology_custom_element_metrics] EXCEPTION: " + str(exc), "info")

            return

    def report_topology_custom_group_metrics(self, group, metrics):
        return

    def log(self, msg, type):
        if self.args['debug'] == "true":
            if type == "info":
                logger.info(msg)
            if type == "error":
                logger.error(msg)

            if self.args['dev'] == "true":
                print(msg)

        return


#class Test:
#    @staticmethod
#    def test1():
#        id = "cluster-1"
#        url = "https://11.222.333.444"
#        token = "eyAbcdehbGciOiJSUzI1NiIsInxxwcCI6IkpXVCJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJkeW5hdHJhY2UtdG9rZW4tdGhscWoiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiZHluYXRyYWNlIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiZjRlM2UxN2UtYmIyMS0xMWU4LTkyMDMtNDIwMTBhODAwZmY2Iiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50Omt1YmUtc3lzdGVtOmR5bmF0cmFjZSJ9.Zf8Sj6amIkLBK0Kq1Yneg85eEja-YvlosHxCiC1lk0bo2Es_sFxQCRvlwrQ0XQ3axUSEF8y9rxO3DRib7v4NL3yVkuB3TsJ8J7rtF9Rvg1Tq4Lwyvt2XgbNIsOMvAOR98i8h6taMx38OaP-wrc8UvFbiq4AMXXoskR99ekCx7QZsGkoQ8jp1c9jxcBNPwYrjxg13uIHlDZNPXqE7z_dgtncSzLeX_dlQNZgi8G-mJ51ZHeV3f-rV5cF762fHqVl_QBOCWSOudwPNAMxJpydFumeGJi3GmAAlZ7sSfx_YpWFsPjyORP3HcEL6AiXZFQTCICmEIfv_Jmxv_L5FH3m6OQ"
#        json_config = json.load(open("plugin.json"))
#        plugin = RemoteKubernetesPlugin()
#        plugin.initialize(config={"id": id, "url": url, "token": token, "debug": "true", "dev": "true"}, json_config=json_config)
#        plugin.query()
#
#
#if __name__ == "__main__":
#    Test.test1()
