# CRAB Data Pipeline

The crab_cronjob is material for excuting schedule datapipeline. The goal is to update the data on dashboards(Opensearch and Grafana) daily.

The folder contains \
**crab_tape_recall_daily.py** - Code for pulling rucio history data from HDFS and upload to dashboards daily \
**crab_data_daily.py** - Code for pulling crab data from HDFS and upload to dashboards daily \
**cron_srcipt.sh** - Shell script for running particular cronjob \
**osearch.py** and **secret_opensearch.txt** - Tools for send data to Opensearch Dashboards, they are imported in crab_tape_recall_daily.py

The Opensearch Dashboards index: **crab-tape-recall-daily*** and **crab-tape-recall-daily-ekong*** \
Grafana: https://monit-grafana.cern.ch/goto/9Mf6lXjVz?orgId=11

Where srcipt for **acrontab** is \
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_tape_recall_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_tape_recall_daily.log
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_data_data_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_data_daily.log
