#!/usr/bin/python3 -u
import sys, argparse, subprocess, json, random, traceback, re, time, signal

parser = argparse.ArgumentParser(description='tool to synchronize ceph rbd images between two clusters, using ssh', usage='python3 main.py --source username@server:rbd/image_name --destination rbd_backup/backup_test_destination')

parser.add_argument('-v', '--verbose', action="store_true", dest='verbose', default=False, help='print verbose output')
parser.add_argument('-vv', '--debug', action="store_true", dest='debug', default=False, help='print debug output')
parser.add_argument('-s', '--source', action="store", dest='source', help='the source ceph rbd image: username@server:rbd/image_name', type=str, required=True)
parser.add_argument('-d', '--destination', action="store", dest='destination', help='the destination ceph rbd image: rbd_backup/backup_test_destination', type=str, required=True)
parser.add_argument('-p', '--snapshot-prefix', action="store", dest='snapshotPrefix', help='', required=True, default='backup_snapshot_')
parser.add_argument('-w', '--whole-object', action="store_true", dest='wholeObject', help='do not diff for intra-object deltas. Dramatically improves diff performance but may result in larger delta backup', required=False, default=True)
parser.add_argument('-healty', '--wait-until-healthy', action="store_true", dest='waitHealthy', help='wait until cluster is healthy', required=False, default=True)
parser.add_argument('-no-scrub', '--no-scrubbing', action="store_true", dest='noScrubbing', help='wait for scrubbing to finnish and disable scrubbing (does re-enable scrubbing automatically). This implies --wait-until-healthy', required=False, default=False)

args = parser.parse_args()

LOGLEVEL_DEBUG = 0
LOGLEVEL_INFO = 1
LOGLEVEL_WARN = 2

BACKUPMODE_INITIAL = 1
BACKUPMODE_INCREMENTAL = 2

SNAPSHOT_PREFIX: str = args.snapshotPrefix

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def printStdErr(message: str) -> None:
    print(str, file=sys.stderr)

def logMessage(message: str, level: int) -> None:
    if level <= LOGLEVEL_INFO and not (args.verbose or args.debug): return
    if level == LOGLEVEL_DEBUG and not args.debug: return
    else:
        if level == LOGLEVEL_DEBUG:
            printStdErr(message)
        else:
            print(message)

def sizeof_fmt(num: float, suffix: str ='B') -> str:
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)

def execRaw(command: str) -> str:
    logMessage('exec command "' + command + '"', LOGLEVEL_INFO)
    return str(subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().decode("utf-8")).strip("\n")

def execParseJson(command: str):
    return json.loads(execRaw(command), encoding='UTF-8')

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def cephRbdPathToObject(imagePath: str):
    pathArr = imagePath.split('/')
    return {'pool': pathArr[0], 'image': pathArr[1]}

def cephRbdObjectToPath(object):
    return '/'.join([object['pool'], object['image']])

# ----------------------------------------------------------------------------
# username@server:rbd/image_name
remoteConnectionCommand = args.source.split(':')[0]
source = cephRbdPathToObject(args.source.split(':')[1])
destination = cephRbdPathToObject(args.destination)
# ----------------------------------------------------------------------------

def getCephRbdImages(pool: str, commandInject: str = ''):
    return execParseJson(commandInject + 'rbd -p ' + pool + ' ls --format json')

def cephRbdImageExists(pool: str, image: str, commandInject: str = ''):
    return image in getCephRbdImages(pool, commandInject)

def getCephSnapshots(pool: str, image: str, commandInject: str = ''):
    return execParseJson(commandInject + 'rbd -p ' + pool + ' snap ls --format json ' + image)

def countPreviousCephRbdSnapsots(pool: str, image: str, commandInject: str = ''):
    logMessage('get ceph snapshot count for image ' + image, LOGLEVEL_INFO)
    count = 0
    for snapshot in getCephSnapshots(pool, image, commandInject):
        if (snapshot['name'].startswith(SNAPSHOT_PREFIX, 0, len(SNAPSHOT_PREFIX))):
            count += 1

    return count

def previousCephRbdSnapsotName(pool: str, image: str, commandInject: str = ''):
    logMessage('get ceph snapshot name for image ' + commandInject + pool + '/' + image, LOGLEVEL_INFO)
    for snapshot in getCephSnapshots(pool, image, commandInject):
        if (snapshot['name'].startswith(SNAPSHOT_PREFIX, 0, len(SNAPSHOT_PREFIX))):
            return snapshot['name']
    raise RuntimeError('cannot determine ceph snapshot name, aborting!')

def getBackupMode(source, destination, commandInject: str = ''):
    sourceExists = cephRbdImageExists(source['pool'], source['image'], commandInject)
    if (not sourceExists):
        raise RuntimeError('invalid arguments, source image does not exist ' + cephRbdObjectToPath(source))

    destinationExists = cephRbdImageExists(destination['pool'], destination['image'], commandInject)
    if (not destinationExists):
        raise RuntimeError('invalid arguments, destination image does not exist ' + cephRbdObjectToPath(destination))

    sourcePreviousSnapshotCount = countPreviousCephRbdSnapsots(source['pool'], source['image'], commandInject)

    if (sourcePreviousSnapshotCount > 1):
        raise RuntimeError('inconsistent state, more than one snapshot for image ' + cephRbdObjectToPath(source))

    if (sourcePreviousSnapshotCount == 1 and not destinationExists):
        raise RuntimeError('inconsistent state, source snapshot found but destination does not exist ' + cephRbdObjectToPath(destination))

    if (sourcePreviousSnapshotCount == 0 and destinationExists):
        raise RuntimeError('inconsistent state, source snapshot not found but destination does exist')

    if (sourcePreviousSnapshotCount == 0 and not destinationExists):
        return {'mode': BACKUPMODE_INITIAL}
    else:
        return {'mode': BACKUPMODE_INCREMENTAL, 'base_snapshot': previousCephRbdSnapsotName(source['pool'], source['image'], commandInject)}

def createCephRbdSnapshot(pool: str, image: str, commandInject: str = ''):
    logMessage('creating ceph snapshot for image ' + commandInject + pool + '/' + image, LOGLEVEL_INFO)
    name = SNAPSHOT_PREFIX + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
    logMessage('exec command "' + commandInject + 'rbd -p ' + pool + ' snap create ' + image + '@' + name + '"', LOGLEVEL_INFO)
    if commandInject != '':
        code = subprocess.call(commandInject.strip().split(' ') + ['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    else:
        code = subprocess.call(['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    if (code != 0):
        raise RuntimeError('error creating ceph snapshot code: ' + str(code))
    logMessage('ceph snapshot created ' + name, LOGLEVEL_INFO)
    return name

def removeCephRbdSnapshot(pool: str, image: str, snapshot: str, commandInject: str = ''):
    execRaw(commandInject + 'rbd -p ' + pool + ' snap rm ' + image + '@' + snapshot)

def getCephRbdProperties(pool: str, image: str, commandInject: str = ''):
    return execParseJson('rbd -p ' + pool + ' --format json info ' + image, commandInject)

def setCephScrubbing(enable: bool, commandInject: str = ''):
    actionName = 'enable' if enable else 'disable'
    action = 'set' if enable else 'unset'
    logMessage(actionName + ' ceph scrubbing', LOGLEVEL_INFO)
    execRaw(commandInject + 'ceph osd ' + action + ' nodeep-scrub')
    execRaw(commandInject + 'ceph osd ' + action + ' noscrub')

def waitForCephClusterHealthy(commandInject: str = ''):
    logMessage('waiting for ceph cluster to become healthy', LOGLEVEL_INFO)
    while (execRaw(commandInject + 'ceph health detail').startswith('HEALTH_ERR')):
        print('.', end='', file=sys.stderr)
        time.sleep(5)

def waitForCephScrubbingCompletion(commandInject: str = ''):
    logMessage('waiting for ceph cluster to complete scrubbing', LOGLEVEL_INFO)
    pattern = re.compile("scrubbing")
    while (pattern.search(execRaw(commandInject + 'ceph status'))):
        print('.', end='', file=sys.stderr)
        time.sleep(5)

def cleanup(arg1 = None, arg2 = None, commandInject: str = ''):
    logMessage('cleaning up...', LOGLEVEL_INFO)

    if (args.noScrubbing):
        setCephScrubbing(True, commandInject)

try:
    executeOnRemoteCommand = 'ssh ' + remoteConnectionCommand + ' '
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    mode = getBackupMode(source, destination, executeOnRemoteCommand)

    if (args.waitHealthy or args.noScrubbing):
        waitForCephClusterHealthy()
        waitForCephClusterHealthy(executeOnRemoteCommand)
    if (args.noScrubbing):
        setCephScrubbing(False)
        setCephScrubbing(False, executeOnRemoteCommand)
        waitForCephScrubbingCompletion()
        waitForCephScrubbingCompletion(executeOnRemoteCommand)

    if (mode['mode'] == BACKUPMODE_INITIAL):
        snapshot = createCephRbdSnapshot(source['pool'], source['image'], executeOnRemoteCommand)
        #TODO: create target image
        #createZfsVolume(args.destination, getCephRbdProperties(args.source)['size'])
        #sourcePath = mapCephRbdImage(args.source + '@' + snapshot)
        #size = compareDeviceSize(sourcePath, destinationPath)

        logMessage('beginning full copy from ' + cephRbdObjectToPath(source) + ' to ' + cephRbdObjectToPath(destination), LOGLEVEL_INFO)

        #TODO: start full image copy, using export-diff, to destination
        #TODO: datarate to strderr via command "pv"

        logMessage('copy finished', LOGLEVEL_INFO)
        createCephRbdSnapshot(destination['pool'], destination['image'])
        removeCephRbdSnapshot(source['pool'], source['image'], snapshot, executeOnRemoteCommand)

    if (mode['mode'] == BACKUPMODE_INCREMENTAL):
        snapshot1 = mode['base_snapshot']
        snapshot2 = createCephRbdSnapshot(source['pool'], source['image'], executeOnRemoteCommand)

        logMessage('beginning incremental copy from ' + cephRbdObjectToPath(source) + ' to ' + cephRbdObjectToPath(destination), LOGLEVEL_INFO)

        #TODO: start incremental image copy, using export-diff, to destination
        #TODO: datarate to strderr via command "pv"

        logMessage('copy finished', LOGLEVEL_INFO)
        createCephRbdSnapshot(destination['pool'], destination['image'])
        removeCephRbdSnapshot(source['pool'], source['image'], snapshot, executeOnRemoteCommand)

    logMessage(bcolors.OKGREEN + 'Done with ' + cephRbdObjectToPath(source) + ' -> ' + cephRbdObjectToPath(destination) + bcolors.ENDC, LOGLEVEL_INFO)


except KeyboardInterrupt:
    logMessage(bcolors.WARNING + 'Interrupt, terminating...' + bcolors.ENDC, LOGLEVEL_WARN)

except RuntimeError as e:
    logMessage(bcolors.FAIL + 'runtime exception ' + str(e) + bcolors.ENDC, LOGLEVEL_WARN)

except Exception as e:
    logMessage(bcolors.FAIL + 'unexpected exception (probably a bug): ' + str(e) + bcolors.ENDC, LOGLEVEL_WARN)
    traceback.print_exc()

finally:
    cleanup()
