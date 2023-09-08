import hashlib
from datetime import datetime, timezone
import shutil
import os
import sys
from time import sleep

srcFiles = []
replicaFiles = []
src_path = sys.argv[1] if len(sys.argv) > 1 else '.'
replica_path = sys.argv[2] if len(sys.argv) > 2 else '.'


def retrieveFileList():
    srcFiles = os.listdir(src_path)
    replicaFiles = os.listdir(replica_path)
    compare_source_and_replice(srcFiles, replicaFiles)


def compare_source_and_replice(srcFiles, replicaFiles):
    if(len(replicaFiles) > 0):
        mismatchFiles = [b for a, b in zip(sorted(srcFiles), sorted(replicaFiles)) if (getMismatchFiles(sys.argv[1], a) != getMismatchFiles(sys.argv[2], b))]
        if len(mismatchFiles) > 0:
            logging(str(mismatchFiles), 'identified this file is mismatching with original file', '', '')
            remove_replica_file([mismatchFiles])
            getMissingFiles(srcFiles, replicaFiles)            
        elif (len(replicaFiles) != len(srcFiles)):
            getMissingFiles(srcFiles, replicaFiles)
        else:
            pass            
    else:
        create_replica_file(srcFiles)
    
    sleep(30)
    logging('','Sleeping for 10 seconds')
    retrieveFileList()


def getMissingFiles(srcFiles, replicaFiles):
    missingFilesInReplica = [a for a in srcFiles if a not in replicaFiles]
    logging(str(missingFilesInReplica), 'files missing, needs to be created', '', '')
    create_replica_file(missingFilesInReplica)


def getMismatchFiles(folderpath, filename, block_size=2**20):
    md5 = hashlib.md5()
    filepath = os.path.join(folderpath, filename)
    with open(filepath, "rb") as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
    return (md5.digest())


def create_replica_file(filesToCreate):
    for file in filesToCreate:
        srcPath = os.path.join(sys.argv[1], file)
        shutil.copy2(srcPath, sys.argv[2])
        logging(file, 'has been moved from', sys.argv[1],' to ', sys.argv[2])

def remove_replica_file(filesToDelete):
    for file in filesToDelete:
        path = os.path.join(sys.argv[2], file[0])
        os.remove(path)
        logging(file[0], 'has been Deleted from', '', sys.argv[2])

def logging(filename='', message='', srcFldr='',message1='' ,replicaFldr=''):
    now = datetime.now()
    CurrentData = now.strftime("%d/%m/%Y %H:%M:%S ")
    with open(sys.argv[3], mode='a') as f:
        writingText = CurrentData+': '+filename + ' ' + message + srcFldr +message1+ replicaFldr + ' \n'
        f.write(writingText)
    f.close()


retrieveFileList()
