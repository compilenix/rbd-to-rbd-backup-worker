#!/usr/bin/python3 -u
import argparse
import json
import random
import re
import signal
import subprocess
import sys
import time
import traceback

parser = argparse.ArgumentParser(description='tool to synchronize ceph rbd images between two clusters, using ssh', usage='python3 main.py --source username@server:rbd/image_name --destination rbd_backup/backup_test_destination')

parser.add_argument('-v', '--verbose', action="store_true", dest='verbose', default=False, help='print verbose output')
parser.add_argument('-vv', '--debug', action="store_true", dest='debug', default=False, help='print debug output')
parser.add_argument('-s', '--source', action="store", dest='source', help='the source ceph rbd image: username@server:rbd/image_name', type=str, required=True)
parser.add_argument('-d', '--destination', action="store", dest='destination', help='the destination ceph rbd image: rbd_backup/backup_test_destination', type=str, required=True)
parser.add_argument('-p', '--snapshot-prefix', action="store", dest='snapshotPrefix', help='', required=True, default='backup_snapshot_')
parser.add_argument('-w', '--whole-object', action="store_true", dest='wholeObject', help='do not diff for intra-object deltas. Dramatically improves diff performance but may result in larger delta backup', required=False, default=True)
parser.add_argument('-healthy', '--wait-until-healthy', action="store_true", dest='waitHealthy', help='wait until cluster is healthy', required=False, default=True)
parser.add_argument('-no-scrub', '--no-scrubbing', action="store_true", dest='noScrubbing', help='wait for scrubbing to finnish and disable scrubbing (does re-enable scrubbing automatically). This implies --wait-until-healthy', required=False, default=False)

args = parser.parse_args()

LOGLEVEL_DEBUG = 0
LOGLEVEL_INFO = 1
LOGLEVEL_WARN = 2

BACKUPMODE_INITIAL = 1
BACKUPMODE_INCREMENTAL = 2

SNAPSHOT_PREFIX: str = args.snapshotPrefix


class BackgroundColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_std_err(message: str) -> None:
    print(str, file=sys.stderr)


def log_message(message: str, level: int) -> None:
    if level <= LOGLEVEL_INFO and not (args.verbose or args.debug): return
    if level == LOGLEVEL_DEBUG and not args.debug: return
    else:
        if level == LOGLEVEL_DEBUG:
            print_std_err(message)
        else:
            print(message)


def sizeof_fmt(num: float, suffix: str ='B') -> str:
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def exec_raw(command: str) -> str:
    log_message('exec command "' + command + '"', LOGLEVEL_INFO)
    return str(subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().decode("utf-8")).strip("\n")


def exec_parse_json(command: str):
    return json.loads(exec_raw(command), encoding='UTF-8')

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------


def ceph_rbd_path_to_object(image_path: str):
    path_arr = image_path.split('/')
    return {'pool': path_arr[0], 'image': path_arr[1]}


def ceph_rbd_object_to_path(obj):
    return '/'.join([obj['pool'], obj['image']])

# ----------------------------------------------------------------------------
# username@server:rbd/image_name


remoteConnectionCommand = args.source.split(':')[0]
source = ceph_rbd_path_to_object(args.source.split(':')[1])
destination = ceph_rbd_path_to_object(args.destination)


# ----------------------------------------------------------------------------


def get_ceph_rbd_images(pool: str, command_inject: str = ''):
    return exec_parse_json(command_inject + 'rbd -p ' + pool + ' ls --format json')


def ceph_rbd_image_exists(pool: str, image: str, command_inject: str = ''):
    return image in get_ceph_rbd_images(pool, command_inject)


def get_ceph_snapshots(pool: str, image: str, command_inject: str = ''):
    return exec_parse_json(command_inject + 'rbd -p ' + pool + ' snap ls --format json ' + image)


def count_previous_ceph_rbd_snapsots(pool: str, image: str, command_inject: str = ''):
    log_message('get ceph snapshot count for image ' + image, LOGLEVEL_INFO)
    count = 0
    for current_snapshot in get_ceph_snapshots(pool, image, command_inject):
        if current_snapshot['name'].startswith(SNAPSHOT_PREFIX, 0, len(SNAPSHOT_PREFIX)):
            count += 1

    return count


def previous_ceph_rbd_snapsot_name(pool: str, image: str, command_inject: str = ''):
    log_message('get ceph snapshot name for image ' + command_inject + pool + '/' + image, LOGLEVEL_INFO)
    for current_snapshot in get_ceph_snapshots(pool, image, command_inject):
        if current_snapshot['name'].startswith(SNAPSHOT_PREFIX, 0, len(SNAPSHOT_PREFIX)):
            return current_snapshot['name']
    raise RuntimeError('cannot determine ceph snapshot name, aborting!')


def get_backup_mode(from_source, to_destination, command_inject: str = ''):
    source_exists = ceph_rbd_image_exists(from_source['pool'], from_source['image'], command_inject)
    if not source_exists:
        raise RuntimeError('invalid arguments, source image does not exist ' + ceph_rbd_object_to_path(from_source))

    destination_exists = ceph_rbd_image_exists(to_destination['pool'], to_destination['image'], command_inject)
    if not destination_exists:
        raise RuntimeError('invalid arguments, destination image does not exist ' + ceph_rbd_object_to_path(to_destination))

    source_previous_snapshot_count = count_previous_ceph_rbd_snapsots(from_source['pool'], from_source['image'], command_inject)

    if source_previous_snapshot_count > 1:
        raise RuntimeError('inconsistent state, more than one snapshot for image ' + ceph_rbd_object_to_path(from_source))

    if source_previous_snapshot_count == 1 and not destination_exists:
        raise RuntimeError('inconsistent state, source snapshot found but destination does not exist ' + ceph_rbd_object_to_path(to_destination))

    if source_previous_snapshot_count == 0 and destination_exists:
        raise RuntimeError('inconsistent state, source snapshot not found but destination does exist')

    if source_previous_snapshot_count == 0 and not destination_exists:
        return {'mode': BACKUPMODE_INITIAL}
    else:
        return {'mode': BACKUPMODE_INCREMENTAL, 'base_snapshot': previous_ceph_rbd_snapsot_name(from_source['pool'], from_source['image'], command_inject)}


def create_ceph_rbd_snapshot(pool: str, image: str, command_inject: str = ''):
    log_message('creating ceph snapshot for image ' + command_inject + pool + '/' + image, LOGLEVEL_INFO)
    name = SNAPSHOT_PREFIX + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
    log_message('exec command "' + command_inject + 'rbd -p ' + pool + ' snap create ' + image + '@' + name + '"', LOGLEVEL_INFO)
    if command_inject != '':
        code = subprocess.call(command_inject.strip().split(' ') + ['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    else:
        code = subprocess.call(['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    if code != 0:
        raise RuntimeError('error creating ceph snapshot code: ' + str(code))
    log_message('ceph snapshot created ' + name, LOGLEVEL_INFO)
    return name


def remove_ceph_rbd_snapshot(pool: str, image: str, snapshot: str, command_inject: str = ''):
    exec_raw(command_inject + 'rbd -p ' + pool + ' snap rm ' + image + '@' + snapshot)


def get_ceph_rbd_properties(pool: str, image: str, command_inject: str = ''):
    return exec_parse_json('rbd -p ' + pool + ' --format json info ' + image, command_inject)


def set_ceph_scrubbing(enable: bool, command_inject: str = ''):
    action_name = 'enable' if enable else 'disable'
    action = 'set' if enable else 'unset'
    log_message(action_name + ' ceph scrubbing', LOGLEVEL_INFO)
    exec_raw(command_inject + 'ceph osd ' + action + ' nodeep-scrub')
    exec_raw(command_inject + 'ceph osd ' + action + ' noscrub')


def wait_for_ceph_cluster_healthy(command_inject: str = ''):
    log_message('waiting for ceph cluster to become healthy', LOGLEVEL_INFO)
    while exec_raw(command_inject + 'ceph health detail').startswith('HEALTH_ERR'):
        print('.', end='', file=sys.stderr)
        time.sleep(5)


def wait_for_ceph_scrubbing_completion(command_inject: str = ''):
    log_message('waiting for ceph cluster to complete scrubbing', LOGLEVEL_INFO)
    pattern = re.compile("scrubbing")
    while pattern.search(exec_raw(command_inject + 'ceph status')):
        print('.', end='', file=sys.stderr)
        time.sleep(5)


def cleanup(arg1 = None, arg2 = None, command_inject: str = ''):
    log_message('cleaning up...', LOGLEVEL_INFO)

    if args.noScrubbing:
        set_ceph_scrubbing(True, command_inject)


try:
    executeOnRemoteCommand = 'ssh ' + remoteConnectionCommand + ' '
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    mode = get_backup_mode(source, destination, executeOnRemoteCommand)

    if args.waitHealthy or args.noScrubbing:
        wait_for_ceph_cluster_healthy()
        wait_for_ceph_cluster_healthy(executeOnRemoteCommand)
    if args.noScrubbing:
        set_ceph_scrubbing(False)
        set_ceph_scrubbing(False, executeOnRemoteCommand)
        wait_for_ceph_scrubbing_completion()
        wait_for_ceph_scrubbing_completion(executeOnRemoteCommand)

    if mode['mode'] == BACKUPMODE_INITIAL:
        snapshot = create_ceph_rbd_snapshot(source['pool'], source['image'], executeOnRemoteCommand)
        # TODO: create target image
        #createZfsVolume(args.destination, getCephRbdProperties(args.source)['size'])
        #sourcePath = mapCephRbdImage(args.source + '@' + snapshot)
        #size = compareDeviceSize(sourcePath, destinationPath)

        log_message('beginning full copy from ' + ceph_rbd_object_to_path(source) + ' to ' + ceph_rbd_object_to_path(destination), LOGLEVEL_INFO)

        # TODO: start full image copy, using export-diff, to destination
        # TODO: datarate to strderr via command "pv"

        log_message('copy finished', LOGLEVEL_INFO)
        create_ceph_rbd_snapshot(destination['pool'], destination['image'])
        remove_ceph_rbd_snapshot(source['pool'], source['image'], snapshot, executeOnRemoteCommand)

    if mode['mode'] == BACKUPMODE_INCREMENTAL:
        snapshot1 = mode['base_snapshot']
        snapshot2 = create_ceph_rbd_snapshot(source['pool'], source['image'], executeOnRemoteCommand)

        log_message('beginning incremental copy from ' + ceph_rbd_object_to_path(source) + ' to ' + ceph_rbd_object_to_path(destination), LOGLEVEL_INFO)

        # TODO: start incremental image copy, using export-diff, to destination
        # TODO: datarate to strderr via command "pv"

        log_message('copy finished', LOGLEVEL_INFO)
        create_ceph_rbd_snapshot(destination['pool'], destination['image'])
        remove_ceph_rbd_snapshot(source['pool'], source['image'], snapshot, executeOnRemoteCommand)

    log_message(BackgroundColors.OKGREEN + 'Done with ' + ceph_rbd_object_to_path(source) + ' -> ' + ceph_rbd_object_to_path(destination) + BackgroundColors.ENDC, LOGLEVEL_INFO)


except KeyboardInterrupt:
    log_message(BackgroundColors.WARNING + 'Interrupt, terminating...' + BackgroundColors.ENDC, LOGLEVEL_WARN)

except RuntimeError as e:
    log_message(BackgroundColors.FAIL + 'runtime exception ' + str(e) + BackgroundColors.ENDC, LOGLEVEL_WARN)

except Exception as e:
    log_message(BackgroundColors.FAIL + 'unexpected exception (probably a bug): ' + str(e) + BackgroundColors.ENDC, LOGLEVEL_WARN)
    traceback.print_exc()

finally:
    cleanup()
