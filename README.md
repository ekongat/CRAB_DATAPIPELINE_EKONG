# CRAB_DATAPIPELINE

The crab_cronjob is material for excuting schedule job, it contains
**crab_tape_recall_daily.py** - Code for pulling rucio history data from HDFS and upload to Opensearch Dashboards daily
**cron_srcipt.sh** - Shell script for running cronjob
**osearch.py** and **secret_opensearch.txt** - Tools for send data to Opensearch Dashboards, they are imported in crab_tape_recall_daily.py

The Opensearch Dashboards index: **test-crab-tape-recall-daily***

Where srcipt for **acrontab** is
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjocronjob/crab_tape_recall_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_tape_recall_daily.log
