# coding: utf-8
import time
import json
import requests
from OpenTSDBWrite import OpenTSDBWrite


class YarnJmxUtil(object):
    queue_metrics_list = [
        "UsedAMResourceMB", # 当前AM占用的内存
        "UsedAMResourceVCores", # 当前AM占用的CPU
        "AppsRunning", # 当前在运行的APPs
        "AppsPending", # 当前等待调度Apps
        "AppsCompleted", # 当前已经完成Apps
        "AllocatedMB", # 当前占用内存
        "AllocatedVCores", # 当前占用CPU
        "AllocatedContainers", # 当前分配Container数量
        "AvailableMB", # 当前有效剩余内存
        "AvailableVCores", # 当前有效剩余CPU
        "PendingMB", # 当前等待调度需要的内存
        "PendingVCores", # 当前等待调度需要的CPU
        "PendingContainers", # 当前等待调度需要的Container
        "ReservedMB", # 当前预留内存
        "ReservedVCores", # 当前预留CPU
        "ReservedContainers" # 当前预留Container
    ]

    @staticmethod
    def query_queue_metrics(queue_name):
        """
        :param queue_name: ex root.etl_high
        :return:
        """

        queue_level_list = queue_name.split(".")
        qry_params = "Hadoop:service=ResourceManager,name=QueueMetrics"
        for i in xrange(0, len(queue_level_list)):
            qry_params += ",q%s=%s" % (i, queue_level_list[i])
        url = "http://localhost:8088/jmx?qry=%s" % qry_params
        response = requests.get(url).content
        response_beans = json.loads(response)["beans"]

        queue_metrics_dict = {}
        for response_dict in response_beans:
            if response_dict["name"] == qry_params:
                for queue_metrics in YarnJmxUtil.queue_metrics_list:
                    queue_metrics_dict[queue_metrics] = response_dict[queue_metrics]
        return queue_metrics_dict

    @staticmethod
    def insert_opentsdb(queue_name, queue_metrics_dict):
        insert_data_list = []
        timestamp = int(time.time())
        for queue_metrics in queue_metrics_dict:
            opentsdb_metric_dict = {
                "metric": "yarn.queue.%s.%s" % (queue_name, queue_metrics),
                "timestamp": timestamp,
                "value": queue_metrics_dict[queue_metrics],
                "tags": {"type": "yarn.queue"}
            }
            insert_data_list.append(opentsdb_metric_dict)

        OpenTSDBWrite.write_db(insert_data_list)

    @staticmethod
    def run(queue_name):
        queue_metrics_dict = YarnJmxUtil.query_queue_metrics(queue_name)
        YarnJmxUtil.insert_opentsdb(queue_name, queue_metrics_dict)


if __name__ == "__main__":
    YarnJmxUtil.run("root")
    YarnJmxUtil.run("root.pcsjob")
    YarnJmxUtil.run("root.etl_high")
    YarnJmxUtil.run("root.etl_hour")
    YarnJmxUtil.run("root.pcsalgo")
    YarnJmxUtil.run("root.crm")
    YarnJmxUtil.run("root.default")

