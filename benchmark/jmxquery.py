"""
    Python interface to JMX. Uses local jar to pass commands to JMX and read JSON
    results returned.
"""

import subprocess
import os
import json
from threading import Timer
import logging

# Full Path to Jar
JAR_PATH = os.path.dirname(os.path.realpath(__file__)) + '/JMXQuery-0.1.7.jar'
# Default Java path
DEFAULT_JAVA_PATH = 'java'
# Default timeout for running jar in seconds
DEFAULT_JAR_TIMEOUT = 10

logger = logging.getLogger(__name__)


class JMXQuery:
    """
    A JMX Query which is used to fetch specific MBean attributes/values from the JVM. The object_name can support wildcards
    to pull multiple metrics at once, for example '*:*' will bring back all MBeans and attributes in the JVM with their values.

    You can set a metric name if you want to override the generated metric name created from the MBean path
    """

    def __init__(self,
                 mBeanName,
                 attribute=None,
                 attributeKey=None,
                 value=None,
                 value_type=None,
                 metric_name=None,
                 metric_labels=None):

        self.mBeanName = mBeanName
        self.attribute = attribute
        self.attributeKey = attributeKey
        self.value = value
        self.value_type = value_type
        self.metric_name = metric_name
        self.metric_labels = metric_labels

    def to_query_string(self):
        """
        Build a query string to pass via command line to JMXQuery Jar

        :return:    The query string to find the MBean in format:

                        {mBeanName}/{attribute}/{attributeKey}

                    Example: java.lang:type=Memory/HeapMemoryUsage/init
        """
        query = ""
        if self.metric_name:
            query += self.metric_name

            if (self.metric_labels is not None) and (len(self.metric_labels) > 0):
                query += "<"
                keyCount = 0
                for key, value in self.metric_labels.items():
                    query += key + "=" + value
                    keyCount += 1
                    if keyCount < len(self.metric_labels):
                        query += ","
                query += ">"
            query += "=="

        query += self.mBeanName
        if self.attribute:
            query += "/" + self.attribute
        if self.attributeKey:
            query += "/" + self.attributeKey

        return query

    def to_string(self):

        string = ""
        if self.metric_name:
            string += self.metric_name

            if (self.metric_labels is not None) and (len(self.metric_labels) > 0):
                string += " {"
                keyCount = 0
                for key, value in self.metric_labels.items():
                    string += key + "=" + value
                    keyCount += 1
                    if keyCount < len(self.metric_labels):
                        string += ","
                string += "}"
        else:
            string += self.mBeanName
            if self.attribute:
                string += "/" + self.attribute
            if self.attributeKey:
                string += "/" + self.attributeKey

        string += " = "
        string += str(self.value) + " (" + self.value_type + ")"

        return string


class JMXConnection(object):
    """
    The main class that connects to the JMX endpoint via a local JAR to run queries
    """

    def __init__(self, connection_uri, jmx_username=None, jmx_password=None, java_path=DEFAULT_JAVA_PATH):
        """
        Creates instance of JMXQuery set to a specific connection uri for the JMX endpoint

        :param connection_uri:  The JMX connection URL. E.g.  service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi
        :param jmx_username:    (Optional) Username if JMX endpoint is secured
        :param jmx_password:    (Optional) Password if JMX endpoint is secured
        :param java_path:       (Optional) Provide an alternative Java path on the machine to run the JAR.
                                Default is 'java' which will use the machines default JVM
        """
        self.connection_uri = connection_uri
        self.jmx_username = jmx_username
        self.jmx_password = jmx_password
        self.java_path = java_path

    def __run_jar(self, queries, timeout):
        """
        Run the JAR and return the results

        :param query:   The query
        :return:        The full command array to run via subprocess
        """

        command = [self.java_path, '-jar', JAR_PATH, '-url', self.connection_uri, "-json"]
        if self.jmx_username:
            command.extend(["-u", self.jmx_username, "-p", self.jmx_password])

        queryString = ""
        for query in queries:
            queryString += query.to_query_string() + ";"

        command.extend(["-q", queryString])
        logger.debug("Running command: " + str(command))

        jsonOutput = "[]"
        try:
            stdout = ''
            func = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            kill = lambda process: process.kill()
            timer = Timer(timeout, kill, [func])
            try:
                timer.start()
                stdout, stderr = func.communicate()
            finally:
                jsonOutput = stdout.decode('utf-8')
                timer.cancel()
        except subprocess.CalledProcessError as err:
            logger.error("Error calling JMX: " + err.output.decode('utf-8'))
            raise err

        logger.debug("JSON Output Received: " + jsonOutput)
        metrics = self.__load_from_json(jsonOutput)
        return metrics

    def __load_from_json(self, jsonOutput):
        """
        Loads the list of returned metrics from JSON response

        :param jsonOutput:  The JSON Array returned from the command line
        :return:            An array of JMXQuerys
        """
        jsonMetrics = json.loads(jsonOutput)
        metrics = []
        for jsonMetric in jsonMetrics:
            mBeanName = jsonMetric['mBeanName']
            attribute = jsonMetric['attribute']
            attributeType = jsonMetric['attributeType']
            metric_name = None
            if 'metricName' in jsonMetric:
                metric_name = jsonMetric['metricName']
            metric_labels = None
            if 'metricLabels' in jsonMetric:
                metric_labels = jsonMetric['metricLabels']
            attributeKey = None
            if 'attributeKey' in jsonMetric:
                attributeKey = jsonMetric['attributeKey']
            value = None
            if 'value' in jsonMetric:
                value = jsonMetric['value']

            metrics.append(
                JMXQuery(mBeanName, attribute, attributeKey, value, attributeType, metric_name, metric_labels))
        return metrics

    def query(self, queries, timeout=DEFAULT_JAR_TIMEOUT):
        """
        Run a list of JMX Queries against the JVM and get the results

        :param queries:     A list of JMXQuerys to query the JVM for
        :return:            A list of JMXQuerys found in the JVM with their current values
        """
        return self.__run_jar(queries, timeout)
