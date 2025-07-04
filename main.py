## Veeam Task 1
## Implement a program that synchronizes two folders: source and replica. 
## The program should maintain a full, identical copy of source folder at replica folder.
##
## 1. Crawl the source folder and find subfolders. (Done)
## 2. Start cloning the files in the source folder to replica. (Done)
## 3. Remove any files & folders that aren't in the source folder anymore. (Done)
## 4. Perform a MD5 check on source folder and replica folder. (Done)
## 5. Setup a sync interval. It is in seconds and has a limit of syncs done. (Done)
## 6. Make a log function that will store all the actions performed with timestamps. (Done)
## 
## Question?
## Should we handle empty directories. In the replica directory we should remove any empty directories or just make an exact copy.
## ANSWER: We just copy the directory even if empty and remove it if it's not in the source directory.
## 
## Question?
## The sync intervals are set in seconds with a for loop rather than using the third-party library "schedule". Is the loop supposed to use seconds, minutes or hours?
## I am not sure if I am allowed to use third-party libraries including for CLI frameworks.
## ANSWER: We keep it simple, no third-party libraries and we will use seconds for the intervals.
##

import os
import hashlib
import time
import sys

from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
GLOBAL_LOG_PATH = BASE_DIR # The path to the log file (Default to current running directory)

def getFolders(sourcePath: str) -> list:
    """ This function is for building a list of all the paths for each file in the source folder. """
    
    # Check if the path exists
    if not os.path.exists(sourcePath):
        raise NotADirectoryError(f"Path: \"{sourcePath}\" does not exist! Check your path or try another path.")
    
    # Log
    logger(f"Collecting all folders and files from {sourcePath}")

    # Crawl the parent folder and subfolders of the given path
    result = []
    for root, dirs, files in os.walk(sourcePath):

        # Store all the files in the result list
        for name in files:
            result.append({
                    "path": os.path.join(root, name), # File path name
                    "filename": name, # Name of the file
                    "directory": root.replace(sourcePath, "", 1), # The directory the file is in
                })
            
            # Log
            logger(f"Found file: {os.path.join(root, name)}")

        # Store all the directories
        for d in dirs:
            result.append({
                "path": None,
                "filename": None,
                "directory": os.path.join(root.replace(sourcePath, "", 1), d), # The directory
            })
            
            # Log
            logger(f"Found directory: {os.path.join(root.replace(sourcePath, '', 1), d)}")

    return result


def createCopy(sourcePath: str, replicaPath: str) -> None:
    """ This function is to make copies of 2 files. """
    
    # Log
    logger(f"Creating copy of {sourcePath}")
    
    # Open the file original file in binary
    try:
        file0 = open(file=sourcePath, mode="rb")
    except OSError as err:
        raise OSError(f"An Error occured during cloning of {sourcePath}! Error: {err}")
    
    # Create the file in the replica folder
    try:
        file1 = open(file=replicaPath, mode="wb")
    except OSError as err:
        raise OSError(f"An Error occured during creation of {replicaPath}! Error: {err}")
    
    # Write the data from file 0 to file 1
    for line in iter(lambda: file0.read(1024 * 1024), b""):
        file1.write(line)
    
    # Close the files
    file0.close()
    file1.close()


def cloneSource(replicaPath: str, paths: list) -> bool:
    """ This function is for cloning the source folder. """
    
    logger("Checking source and replica integrity...")
    
    # Check if the replica path exists else create it
    try:

        # Log
        if not os.path.exists(replicaPath):
            logger(f"Replica directory does not exist! Creating replica directory: {replicaPath}")
        
        os.makedirs(replicaPath, exist_ok=True)
    except OSError as err:
        raise OSError(f"An Error occured duing replica folder creation! Error: {err}")
    
    # Iterate through the paths list
    numCreatedFiles = 0
    numCreatedDir = 0
    numMD5pass = 0
    numMD5updates = 0
    for path in paths:
        
        # Create the directory for the file
        if len(path.get('directory')) > 0:
            
            # Log
            if not os.path.exists(f"{replicaPath}/{path.get('directory')}"):
                logger(f"Creating directory in replica: {path.get('directory')}")
                
                # Increment the amount of directories created
                numCreatedDir += 1
            
            try:
                os.makedirs(f"{replicaPath}/{path.get('directory')}", exist_ok=True)
            except OSError as err:
                raise OSError(f"An Error occured during folder creation of {path.get('directory')}! Error: {err}")
            
        # If there is no file to save just skip
        if not path.get("path"):
            continue # Skip
        
        # Check if the file exists already
        if not os.path.isfile(f"{replicaPath}/{path.get('directory')}/{path.get('filename')}"):
            try:
                createCopy(path.get("path"), f"{replicaPath}/{path.get('directory')}/{path.get('filename')}")
                
                # Increment the amount of files created
                numCreatedFiles += 1
            except OSError as err:
                raise OSError(err)
        
        # Perform the MD5 check (This will check for any changes in the files)
        logger(f"MD5 check: {path.get('path')}")
        if not md5Check(path.get("path"), f"{replicaPath}/{path.get('directory')}/{path.get('filename')}"):
            
            # Create a copy of the file if the MD5 check does not match
            try:
                createCopy(path.get("path"), f"{replicaPath}/{path.get('directory')}/{path.get('filename')}")
            except OSError as err:
                raise OSError(err)
            
            # Log
            logger("Copied successfully.")
            
            # Increment the amount of MD5 updates
            numMD5updates += 1
        else:
            
            # Log
            logger("MD5 check successful!")
            
            # Increment the amount of MD5 passes
            numMD5pass += 1
            
    # Log
    logger(f"Files created: {numCreatedFiles} - Directories created: {numCreatedDir} - MD5 Check pass: {numMD5pass} - MD5 Check update: {numMD5updates}")

    # Return true to signal that it was successful
    return True
    

def removeOldies(source: str, replica: str) -> bool:
    """ This function is for removing files and folders that do not exist in the source folder anymore."""
    
    logger("Removing any old files and directories...")
    
    # Crawl the source folder
    sourceFolders = getFolders(source)
    
    # Crawl the replica folder
    replicaFolders = getFolders(replica)
    
    # Iterate through the replica folder
    numFilesRemoved = 0
    numDirRemoved = 0
    for path in replicaFolders:
        
        # Check if the file exists in source
        if not any(str(a.get("path")).replace(source, "", 1) == str(path.get("path")).replace(replica, "", 1) for a in sourceFolders):
            
            # Log
            logger(f"Removing file: {path.get('path')}")
            
            # Remove the file from the replica
            try:
                os.remove(path.get("path"))
            except OSError as err:
                raise OSError(f"An Error occured during file removal of {path.get('path')}! Error: {err}")
            
            # Increment the amount of files removed
            numFilesRemoved += 1
        
        # Check if the directory exists in source
        if not any(str(a.get("directory")).replace(source, "", 1) == str(path.get("directory")).replace(replica, "", 1) for a in sourceFolders):
            
            # Log
            logger(f"Removing directory: {replica}/{path.get(f'directory')}")
            
            # Remove the directory from the replica
            try:
                os.removedirs(f"{replica}/{path.get(f'directory')}")
            except OSError as err:
                raise OSError(f"An Error occured during directory removal of {replica}/{path.get(f'directory')}! Error: {err}")
            
            # Increment the amount of files removed
            numDirRemoved += 1
    
    # Log
    logger(f"Files removed: {numFilesRemoved} - Directories removed: {numDirRemoved}")
    
    return True


def md5Hasher(filePath: str) -> str:
    """ This function gets the md5 hash of the file and returns it. """
    
    # Initialize the hasher
    hasher = hashlib.md5()
    
    # Open the file
    try:
        file = open(file=filePath, mode="rb")
    except OSError as err:
        raise OSError(f"An Error occured during opening of the MD5 hash file of {filePath}! Error: {err}")
    
    # Update the hasher
    for line in iter(lambda: file.read(1024 * 1024), b""):
        hasher.update(line)
    
    # Return the hash
    return hasher.hexdigest()


def md5Check(sourcePath: str, replicaPath: str) -> bool:
    """ This function compares the hashes for each file. """
    
    # Compare each others hashes to determine if 
    # the clone is good and not corrupted
    if md5Hasher(sourcePath) != md5Hasher(replicaPath):
        return False
    return True


def logger(msg: str, level: int = 0) -> str:
    """ Function for logging errors. Levels are INFO (0) WARNING (1) ERRROR (2) """
    
    # Check the level of the log
    msgLevel = "INFO"
    if level == 1:
        msgLevel = "WARNING"
    elif level == 2:
        msgLevel = "ERROR"
    
    # Create a formatted message to store in the log file
    formattedMsg = f"| {msgLevel} | {datetime.strftime(datetime.now(), '%d/%m/%Y, %H:%M:%S')} | {msg}"
    
    # Check if the path to the log file exists
    if not os.path.exists(GLOBAL_LOG_PATH):
        raise NotADirectoryError(f"ERROR: {GLOBAL_LOG_PATH} is invalid!")
    
    # Create the log file if it does not exist
    try:

        # Check if this is the first time creating the log file
        initializingLogFile = os.path.isfile(f"{os.path.join(GLOBAL_LOG_PATH, "log.txt")}")

        # Create/open the log file
        file = open(file=f"{os.path.join(GLOBAL_LOG_PATH, "log.txt")}", mode="a", encoding="utf-8")

        # Add the column information
        if not initializingLogFile:
            file.write("| LEVEL | DATE | MESSAGE\n")

    except OSError as err:
        raise OSError(f"An error occured during log file creation/opening! Error: {err}")

    # Write all the logs back to the file
    print(formattedMsg)
    file.write(formattedMsg + "\n")
    
    # Close the file
    file.close()
    
    return formattedMsg


def process(source: str, replica: str, logs: str = None) -> bool:
    """ This function is each step needed to create a replica of the source directory. """
    
    # Set the path to the log path
    if len(logs) > 0:
        global GLOBAL_LOG_PATH
        GLOBAL_LOG_PATH = logs
    
    # Log
    try:
        logger("Starting backup...")
    except NotADirectoryError as err:
        print(f"Please enter a valid folder for log file! Error: {err}")
        return False
    except OSError as err:
        print(err)
        return False
    
    # Crawl the source folder
    try:
        foundFolders = getFolders(source)
    except NotADirectoryError as err:
        logger(err, 2)
        return False
    
    # Check if we found any files
    if len(foundFolders) == 0:
        logger("Folder is empty! Nothing to backup!", 2)
        return False
    
    # Create the replica
    try:
        if not cloneSource(replica, foundFolders):
            logger("Failed to create the replica folder properly", 2)
            return False
        logger("Done checking integrity.")
    except OSError as err:
        logger(err, 2)
        return False

    # Remove any files that do not exist
    try:
        if not removeOldies(source, replica):
            logger("Failed to remove old files!", 2)
            return False
    except OSError as err:
        logger(err, 2)
        return False

    # Log
    logger("Backup completed.")
    return True
    

def main():
    
    # Check if the user has entered 
    # the amount of arguments required
    if len(sys.argv) <= 5:
        print("""Missing arguments! Must be as follows:
- path to source folder
- path to replica folder
- interval between synchronizations 
- amount of synchronizations 
- path to log file
              """)
        return False

    # Check if the arguments are valid
    try:
        int(sys.argv[3])
    except ValueError as err:
        logger(f"Sync timer must be integer! {err}", 1)
        return False
    try:
        int(sys.argv[4])
    except ValueError as err:
        logger(f"Sync amounts must be integer! {err}", 1)
        return False
    
    if not isinstance(sys.argv[1], str):
        logger("Source path must be string!", 1)
        return False
    if not isinstance(sys.argv[2], str):
        logger("Replica path must be string!", 1)
        return False
    if not isinstance(sys.argv[5], str):
        logger("Log file path must be string!", 1)
        return False

    # Trigger the scheduler
    for i in range(int(sys.argv[4])):
        if not process(source=sys.argv[1], replica=sys.argv[2], logs=sys.argv[5]):
            logger("SOMETHING WENT WRONG! Please check the log file.")
            break
        time.sleep(int(sys.argv[3]))


if __name__ == "__main__":
    main()
