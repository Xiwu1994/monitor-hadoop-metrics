# coding: utf-8
import json
import time
import requests
from OpenTSDBWrite import OpenTSDBWrite


class NameNodeJmxUtil(object):
    nn_metrics_dict = {
        "FSNamesystem": ["CapacityUsedGB","CapacityRemainingGB","BlocksTotal","TotalLoad","FilesTotal","BlockCapacity"],
        "JvmMetrics": ["MemNonHeapUsedM","MemHeapUsedM","GcCountParNew","GcCountConcurrentMarkSweep",
                       "ThreadsBlocked","ThreadsWaiting","ThreadsTimedWaiting"],
        "RpcActivityForPort8020": ["RpcQueueTimeAvgTime","RpcProcessingTimeAvgTime","CallQueueLength","NumOpenConnections"],
        "NameNodeInfo": ["BlockPoolUsedSpace"]
    }

    @staticmethod
    def query_nn_metrics():
        """
        :return:  queue_metrics_dict: {
            "FSNamesystem": {
                "CapacityUsedGB": 666,
                "CapacityRemainingGB": 111,
                ...
            },
            ...
        }
        """
        queue_metrics_dict = {}
        for metrics in NameNodeJmxUtil.nn_metrics_dict:
            qry_params = "Hadoop:service=NameNode,name=%s" % metrics
            url = "localhost:50070/jmx?qry=%s" % qry_params
            response = requests.get(url).content
            response_beans_dict = json.loads(response)["beans"][0]

            queue_metrics_dict.setdefault(metrics, {})
            for metrics_index in NameNodeJmxUtil.nn_metrics_dict[metrics]:
                queue_metrics_dict[metrics].setdefault(metrics_index, response_beans_dict[metrics_index])
        return queue_metrics_dict

    @staticmethod
    def insert_opentsdb(queue_metrics_dict):
        insert_data_list = []
        timestamp = int(time.time())
        for metrics in queue_metrics_dict:
            for metrics_index in queue_metrics_dict[metrics]:
                opentsdb_metric_dict = {
                    "metric": "namenode.%s.%s" % (metrics, metrics_index),
                    "timestamp": timestamp,
                    "value": queue_metrics_dict[metrics][metrics_index],
                    "tags": {"type": "namenode"}
                }
                insert_data_list.append(opentsdb_metric_dict)

        OpenTSDBWrite.write_db(insert_data_list)

    @staticmethod
    def run():
        queue_metrics_dict = NameNodeJmxUtil.query_nn_metrics()
        NameNodeJmxUtil.insert_opentsdb(queue_metrics_dict)


if __name__ == "__main__":
    NameNodeJmxUtil.run()
