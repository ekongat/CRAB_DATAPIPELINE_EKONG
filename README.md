# CRAB Data Pipeline

The **crab_cronjob** is material for excuting schedule datapipeline. The goal is to update wanted data on dashboards(Opensearch and Grafana) daily. \
Since there is no method to delete specific data in Opensearch, when error occurs in dataframe such as data duplication or wrong schema is uploaded, we have to delete entire data by running `DELETE /<es index>` in Dev Tools, and upload them again from the beginning, manually. To do so, you can use material in **crab_manual**, which provides the same functions as **crab_cronjob**, to upload wanted data to the any time range that you want by changing only `start_date` and `end_date`.

## Material
The folder contains code for pulling the data from HDFS and upload to dashboards daily, where \
**crab_tape_recall_daily.py** - Pulls wanted data from `rucio rules history` \
**crab_data_daily.py** - Pulls wanted data from `crab tasks` \
**crab_rules_tape_recall_daily.py** - Pulls wanted data from rucio `dataset locks`, `rses`, and `rules` \
**crab_condor_daily.py** - Pulls wanted data from `condor metric` and `crab tasks` \
with the tools: \
**cron_srcipt.sh** - Shell script for running particular cronjob \
**osearch.py** and **secret_opensearch.txt** - Tools for send data to Opensearch Dashboards

## Dashboard 
Grafana: https://monit-grafana.cern.ch/goto/STBZ3uCVz?orgId=11 \
which connects with es-index: `crab-tape-recall-daily-ekong*`, `crab-data-ekong*`, `crab-tape-recall-rules-ekong*`, and `crab-condor-ekong*`, respectively. \
This dashboard is created to answer the following question: \
**Rucio**
1. How long do tasks stay in “taperecall”?
2. How big (TiB) are the datasets that are recalled from tape?

**CRAB Server**
1. How many tasks are using each crab features? (Split algorithm, Ignorelocality, ScriptExe, GPU)
2. How many tasks each users submit? 
3. How many jobs use ignorelocality?

**Condor Matric**
1. What is wall clock time spent by each CMS data tier and each job type?
2. What is the success rate of the Analysis job type?

## acrontab script
```
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_tape_recall_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_tape_recall_daily.log \
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_data_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_data_daily.log \
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_condor_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_condor_daily.log
55 05 * * * lxplus.cern.ch /afs/cern.ch/user/e/eatthaph/crab_cronjob/cron_script.sh /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_rules_tape_recall_daily.py &> /afs/cern.ch/user/e/eatthaph/crab_cronjob/crab_rules_tape_recall_daily.log
```
