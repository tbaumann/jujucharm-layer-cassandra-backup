from charms.reactive import when, when_not, set_state, hook, remove_state, when_any
from charmhelpers.core.hookenv import service_name, status_set, log
from charmhelpers.core.unitdata import kv
from charmhelpers.core import hookenv
from shutil import copy
import os

bin_file_name = "/usr/local/bin/cassandra-backup-only.py"
cron_format_string = "{} python2.7 {} | gawk '{{ print strftime(\"[%Y-%m-%d %H:%M:%S]\"), $0 }}'  >> /var/log/cassandra/backup.log 2>&1\n"


@when_not('cassandra-backup.installed')
def install_cassandra_backup():
    copy("./bin/cassandra-backup-only.py", bin_file_name)
    set_state('cassandra-backup.installed')


@when('cassandra-backup.installed')
@when_not('cassandra-backup.started')
def started():
    write_cron_file()
    status_set('active', 'Ready')
    set_state('cassandra-backup.started')


@when_any('config.changed.cron-time', 'cassandra-backup.needs-render')
def write_cron_file():
    config = hookenv.config()
    cron_time = config['cron-time']
    app_name = hookenv.service_name()
    cache = kv()
    with open('/etc/cron.d/{}'.format(app_name), 'w') as cron_file:
        cron_file.write(cron_format_string.format(str(cron_time), bin_file_name))
        cron_file.write("# username {}\n".format(cache.get('cassandra-backup.cassandra_user')))
        cron_file.write("# password {}\n".format(cache.get('cassandra-backup.cassandra_password')))
    remove_state('cassandra-backup.needs-render')


@when('database.connected')
def db_changed(cassandra):
    username = ''
    password = ''
    cache = kv()
    log("db_changed Relname: {}".format(cassandra.relation_name))
    for conv in cassandra.conversations():
        log("db_changed Conv")
        username = conv.get_remote('username')
        password = conv.get_remote('password')
        log("db_changed Conv host {}".format(conv.get_remote('host')))
        log("db_changed Conv cluster_name {}".format(conv.get_remote('cluster_name')))

    if username and password:
        log("Casssandra server uses authentication")
        cache.set('cassandra-backup.cassandra_user', username)
        cache.set('cassandra-backup.cassandra_password', password)
    else:
        log("Casssandra server doesn't use authentication")
        cache.unset('cassandra-backup.cassandra_user')
        cache.unset('cassandra-backup.cassandra_password')
    set_state('cassandra-backup.needs-render')


@hook('stop')
def stopped():
    app_name = hookenv.service_name()
    log("{} is stopping. Deleting conf file.".format(app_name))
    try:
        os.remove('/etc/cron.d/{}.conf'.format(app_name))
    except FileNotFoundError:
        pass
    try:
        log("Deleting {}".format(bin_file_name))
        os.remove(bin_file_name)
    except FileNotFoundError:
        pass
