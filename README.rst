harvest-task-watcher
====================

A java hazelcast client that connects with the DataONE synchronization queue and emits messages indicating the size, additions, and removals.

Building
--------

::

  mvn clean package

The resulting jar including all dependencies will be created in the target folder, e.g.:

  target/harvest-task-watcher-1.0-SNAPSHOT-jar-with-dependencies.jar


Operating
---------

The the app is meant to be operated from the commandline and by default will emit messages to ``stdout``.

::

  java -jar target/harvest-task-watcher-1.0-SNAPSHOT-jar-with-dependencies.jar -h

  Usage: HarvestTaskWatcher [-i] [-g=<group_name>] [-m=<metric_name>]
                          [-p=<client_password>] [-q=<queue_name>]
                          [-s=<metric_server>] [-t=<ttl_refresh>]
  -g, --group=<group_name>    Group (hzProcess)
  -i, --info                  Show cluster structures and exit
  -m, --metric=<metric_name>  Name of statsd metric (production.syncqueue,
                                stage.syncqueue, sandbox.syncqueue)
  -p, --password=<client_password>
                              Connection password
  -q, --queue=<queue_name>    Name of queue to watch (hzSyncObjectQueue)
  -s, --metric-server=<metric_server>
                              Statsd server name (measure-unm-1.dataone.org)
  -t, --ttl=<ttl_refresh>     Time to refresh (5000ms)


The app will keep running until "Q<return>" is entered on the terminal it is running, or via Ctrl-c. Either mechanism attempts to invoke an orderly shutdown process.

A typical invocation on the stage environment may be something like::

  java -Dlogback.configurationFile=logback.xml \
    -jar harvest-task-watcher-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -p <hazelcast process group password> \
    -m stage.syncqueue > sync-watch.log &

Since the process is long running, it is best to run it under ``screen`` or a similar utility where the terminal can be detached and later resumed.

The app is intended to be run on a CN in the environment it is monitoring. It does not matter which CN, since the hazelcast queue is distributed and consistent across the CNs.

The app also emits a simple metric indicating the size of the queue over a non-blocking UDP channel to measure-unm-1.dataone.org which provides a statsd endpoint. The name of the metric is uncontrolled, so care should be taken to use the "standard" name for the respective environment (statsd will otherwise create a new channel for the metric). The metric names are:

=========== =====================
Environment Metric Name
=========== =====================
Production  production.syncqueue
Stage       stage.syncqueue
Stage-2     stage2.syncqueue
Sandbox     sandbox.syncqueue
Sandbox2    sandbox2.syncqueue
Dev         dev.syncqueue
Dev-2       dev2.syncqueue
=========== =====================


Logging output uses the `LogBack <https://logback.qos.ch/manual/configuration.html>`_ library, which can be configured through an xml file such as the included ``logback.xml`` file. The logback configuration can be specified at startup by the java property::

  java -Dlogback.configurationFile=logback.xml ...



