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
    
    # Log
    logger(f"Collecting all folders and files from {sourcePath}")
    
    # Check if the path exists
    if not os.path.exists(sourcePath):
        logger(f"Path: \"{sourcePath}\" does not exist! Check your path or try another path.", 2)
        return []

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


def cloneSource(replicaPath: str, paths: list) -> bool:
    """ This function is for cloning the source folder. """
    
    # Check if the replica path exists else create it
    try:
        os.makedirs(replicaPath, exist_ok=True)
    except OSError as err:
        logger(f"An Error occured duing replica folder creation! Error: {err}", 2)
        return False
    
    # Iterate through the paths list
    for path in paths:
        
        # Create the directory for the file
        if len(path.get('directory')) > 0:
            
            # Log
            logger(f"Creating directory in replica: {path.get('directory')}")
            
            try:
                os.makedirs(f"{replicaPath}/{path.get('directory')}", exist_ok=True)
            except OSError as err:
                logger(f"An Error occured during folder creation of {path.get('directory')}! Error: {err}", 2)
                return False
            
        # If there is no file to save just skip
        if not path.get("path"):
            continue # Skip
        
        # Log
        logger(f"Creating copy of {path.get('path')}")
        
        # Open the file original file in binary
        try:
            file0 = open(file=path.get("path"), mode="rb")
        except OSError as err:
            logger(f"An Error occured during cloning of {path.get('path')}! Error: {err}", 2)
            return False
        
        # Create the file in the replica folder
        try:
            file1 = open(file=f"{replicaPath}/{path.get('directory')}/{path.get('filename')}", mode="wb")
        except OSError as err:
            logger(f"An Error occured during creation of {replicaPath}/{path.get('path')}! Error: {err}", 2)
            return False
        
        # Write the data from file 0 to file 1
        for line in file0.readlines():
            file1.write(line)
        
        # Close the files
        file0.close()
        file1.close()
        
        # Perform the MD5 check
        logger(f"MD5 check: {path.get('path')}")
        if not md5Check(path.get("path"), f"{replicaPath}/{path.get('directory')}/{path.get('filename')}"):
            
            # Log
            logger(f"MD5 check failed for: {path.get('path')}", 2)
            
            # Remove the file
            try:
                os.remove(f"{replicaPath}/{path.get('directory')}/{path.get('filename')}")
            except OSError as err:
                logger(f"An Error occured during file removal of {replicaPath}/{path.get('directory')}/{path.get('filename')}! Error: {err}", 2)
                return False # IDEA: Stop the removal or just continue to the next file?
        else:
            
            # Log
            logger("MD5 check successful!")
            
        # Log
        logger("Copied successfully.")

    # Return true to signal that it was successful
    return True
    

def removeOldies(source: str, replica: str) -> bool:
    """ This function is for removing files and folders that do not exist in the source folder anymore."""
    
    # Crawl the source folder
    sourceFolders = getFolders(source)
    
    # Crawl the replica folder
    replicaFolders = getFolders(replica)
    
    # Iterate through the replica folder
    for path in replicaFolders:
        
        # Check if the file exists in source
        if not any(str(a.get("path")).replace(source, "", 1) == str(path.get("path")).replace(replica, "", 1) for a in sourceFolders):
            
            # Log
            logger(f"Removing file: {path.get('path')}")
            
            # Remove the file from the replica
            try:
                os.remove(path.get("path"))
            except OSError as err:
                logger(f"An Error occured during file removal of {path.get('path')}! Error: {err}", 2)
                return False # IDEA: Stop the removal or just continue to the next file?
        
        # Check if the directory exists in source
        if not any(str(a.get("directory")).replace(source, "", 1) == str(path.get("directory")).replace(replica, "", 1) for a in sourceFolders):
            
            # Log
            logger(f"Removing directory: {replica}/{path.get(f'directory')}")
            
            # Remove the directory from the replica
            try:
                os.removedirs(f"{replica}/{path.get(f'directory')}")
            except OSError as err:
                logger(f"An Error occured during directory removal of {replica}/{path.get(f'directory')}! Error: {err}", 2)
                return False # IDEA: Stop the removal or just continue to the next file?
    
    return True


def md5Hasher(filePath: str) -> str:
    """ This function gets the md5 hash of the file and returns it. """
    
    # Initialize the hasher
    hasher = hashlib.md5()
    
    # Open the file
    try:
        file = open(file=filePath, mode="rb")
    except OSError as err:
        logger(f"An Error occured during opening of the MD5 hash file of {filePath}! Error: {err}", 2)
        return False
    
    # Update the hasher
    for line in file.readlines():
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
        raise FileNotFoundError(f"ERROR: {GLOBAL_LOG_PATH} is invalid!")
    
    # Create the log file if it does not exist
    try:
        file = open(file=f"{os.path.join(GLOBAL_LOG_PATH, "log.txt")}", mode="a")
    except FileNotFoundError as err:
        
        # If the log file does not exist yet, create it
        file = open(file=f"{os.path.join(GLOBAL_LOG_PATH, "log.txt")}", mode="w")
        
    except OSError as err:
        print(f"An error occured during log file creation/opening! Error: {err}")
        return ""
    
    # Write all the logs back to the file
    print(formattedMsg)
    file.write(formattedMsg + "\n")
    
    # Close the file
    file.close()
    
    return formattedMsg


def process(source: str, replica: str, logs: str = None) -> bool:
    """ This function is each step needed to create a replica of the source directory. """
    
    # Set the path to the log path
    if logs:
        GLOBAL_LOG_PATH = logs
    
    # Log
    try:
        logger("Starting backup...")
    except FileNotFoundError as err:
        print(f"Please enter a valid folder for log file! Leave empty to use default. Error: {err}")
        return False
    
    # Crawl the source folder
    foundFolders = getFolders(source)
    
    # Check if we found any files
    if len(foundFolders) == 0:
        logger("Folder is empty! Nothing to backup!", 2)
        return False
    
    # Create the replica
    logger("Creating copies...")
    if not cloneSource(replica, foundFolders):
        logger("Failed to create the replica folder properly", 2)
        return False
    logger("Successfully cloned!")

    # Remove any files that do not exist
    logger("Removing any old files and directories...")
    if not removeOldies(source, replica):
        logger("Failed to remove old files!", 2)
        return False
    logger("Successfully removed old files!")

    # Log
    logger("Backup completed.")
    

def main():
    
    # Check if the user has entered 
    # the amount of arguments required
    if len(sys.argv) <= 5:
        print("Missing arguments!")

    # Trigger the scheduler
    for i in range(int(sys.argv[4])):
        process(source=sys.argv[1], replica=sys.argv[2], logs=sys.argv[5])
        time.sleep(int(sys.argv[3]))


if __name__ == "__main__":
    main()
