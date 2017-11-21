#!/usr/bin/env python
import datetime
import subprocess
import os
import shutil
import argparse

backup_dir_base = '/root/database_backup/'
#keyspaces to snapshot
snapshot_keyspaces = ['config_db_uuid', 'DISCOVERY_SERVER', 'system_auth','to_bgp_keyspace', 'system_traces', 'useragent', 'svc_monitor_keyspace', 'dm_keyspace', 'config_webui' ]
cass_datadirs = '/var/lib/cassandra/data/'
#day of the month to keep monthly snapshot
monthly_day = 1
#number of keeped day by day snapshot in a row
clean_delta = 14

def backup(backup_dir_base, snapshot_keyspaces, cass_datadirs,options):
    snapshot_name = 'snap-'+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    snapshot_call =  ['/usr/bin/nodetool', 'snapshot'] + snapshot_keyspaces + ['-t', snapshot_name]
    subprocess.call(snapshot_call)
    for sk in snapshot_keyspaces:
        src = os.path.join(cass_datadirs, sk)
        try:
            for d in next(os.walk(src))[1]:
                src_dir = os.path.join(src, d,'snapshots', snapshot_name)
                dst_dir = dest_dir = os.path.join(backup_dir_base, 'var_lib_cassandra_data', snapshot_name, sk, d)
                #print "Copy data from : {} \nto backup: {}" .format(src_dir, dst_dir)
                shutil.copytree(src_dir, dst_dir)
        except StopIteration:
            print "Dir iterration Error"
    clean_snapshot_call =  ['/usr/bin/nodetool', 'clearsnapshot'] + snapshot_keyspaces + ['-t', snapshot_name] + options
    subprocess.call(clean_snapshot_call)
    return (snapshot_name,os.path.join(backup_dir_base, 'var_lib_cassandra_data', snapshot_name))

def clean_old_snapshots(backup_dir_base, monthly_day, clean_delta):
    for s in next(os.walk(os.path.join(backup_dir_base,'var_lib_cassandra_data')))[1]:
        l_day = datetime.datetime(year=int(s[5:9]), month=int(s[9:11]), day=int(s[11:13]))
        l_delta = datetime.datetime.today() - l_day
        if int(l_day.day) == monthly_day:
            print  "Will NOT delete the snapshot {} from {} because is monthly snapshot. Every {} day of the month will be stored." .format(s, l_day, l_day.day)
        if int(l_delta.days) > clean_delta and int(l_day.day) != monthly_day:
            print "Delete the snapshot {} {} days old, treshold {} days" .format(os.path.join(backup_dir_base,'var_lib_cassandra_data',s), l_delta.days, clean_delta)
            shutil.rmtree(os.path.join(backup_dir_base,'var_lib_cassandra_data',s), ignore_errors=True)

parser = argparse.ArgumentParser()
parser.add_argument("--cassandra-user", help="optional cassandra user")
parser.add_argument("--cassandra-password", help="optional cassandra password")
args = parser.parse_args()
if args.cassandra_user != None and args.cassandra_password != None :
    options = [' --password ', args.cassandra_password, ' --username ', args.cassandra_user,]
else:
    options = []
n, d = backup(backup_dir_base, snapshot_keyspaces, cass_datadirs, options)
print "Database was snapshoted as {} and copied to {}" .format(n,d)
clean_old_snapshots(backup_dir_base, monthly_day, clean_delta)
