####################################################################################################
#! /bin/bash
####################################################################################################
# Universal shell script to call Impala sql script
# The SQL needs to develop in such ways that it accepts the parameters from the shell
####################################################################################################
# Version Control (MM/DD/YYYY):
#  12/06/2016 SN: Initial Version
####################################################################################################

# Initialize the value
n=1 # How delay we will get the data
year=$(date --d="today"  +"%Y")
month=$(date --d="today"  +"%m")
day=$(date --d="-$n days" +"%d")

for job_sql in "$@"
do
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] Start run Impala SQL : $job_sql"
        impala-shell -i impala.prd.sg1.tapad.com --var=year=$year --var=month=$month --var=day=$day -f $job_sql
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $job_sql run completed"
done


####################################################################################################
# Example of SQL script:
# select * from default.id_syncs where year=${var:year} and month=${var:month} and day=${var:day} limit 10
####################################################################################################


## Example of crontab usage (crontab -e)
#00 12 6 * * run_sql.sh test.sql
