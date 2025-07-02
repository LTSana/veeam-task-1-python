## Veeam Task 1
## Implement a program that synchronizes two folders: source and replica. 
## The program should maintain a full, identical copy of source folder at replica folder.
##
## 1. Crawl the source folder and find subfolders. (Done)
## 2. Start cloning the files in the source folder to replica. (Done)
## 3. Remove any files & folders that aren't in the source folder anymore. (Done)
## 4. Perform a MD5 check on source folder and replica folder.
## 5. Setup a sync interval. (5 minutes?)
## 6. Make a log function that will store all the actions performed with timestamps.
## 
## Question?
## Should we handle empty directories. In the replica directory we should remove any empty directories or just make an exact copy

import os
import hashlib

def getFolders(sourcePath: str) -> list:
    """ This function is for building a list of all the paths for each file in the source folder. """
    
    # Check if the path exists
    if not os.path.exists(sourcePath):
        print(f"Path: \"{sourcePath}\" does not exist! Check your path or try another path.")
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
            
        # Store all the directories
        for d in dirs:
            result.append({
                "path": None,
                "filename": None,
                "directory": os.path.join(root.replace(sourcePath, "", 1), d), # The directory
            })

    return result


def cloneSource(replicaPath: str, paths: list) -> bool:
    """ This function is for cloning the source folder. """
    
    # Check if the replica path exists else create it
    try:
        os.makedirs(replicaPath, exist_ok=True)
    except OSError as err:
        print(f"An Error occured duing replica folder creation! Error: {err}")
        return False
    
    # Iterate through the paths list
    for path in paths:
        
        # Create the directory for the file
        if len(path.get('directory')) > 0:
            try:
                os.makedirs(f"{replicaPath}/{path.get('directory')}", exist_ok=True)
            except OSError as err:
                print(f"An Error occured during folder creation of {path.get('directory')}! Error: {err}")
                return False
            
        # If the there is no file to save just skip
        if not path.get("path"):
            continue # Skip
        
        # Open the file original file in binary
        try:
            file0 = open(file=path.get("path"), mode="rb")
        except OSError as err:
            print(f"An Error occured during cloning of {path.get('path')}! Error: {err}")
            return False
        
        # Create the file in the replica folder
        try:
            file1 = open(file=f"{replicaPath}/{path.get('directory')}/{path.get('filename')}", mode="wb")
        except OSError as err:
            print(f"An Error occured during creation of {replicaPath}/{path.get('path')}! Error: {err}")
            return False
        
        # Write the data from file 0 to file 1
        for line in file0.readlines():
            file1.write(line)
        
        # Close the files
        file0.close()
        file1.close()
        
        # Perform the MD5 check
        print("Performing MD5 check...")
        if not md5Check(path.get("path"), f"{replicaPath}/{path.get('directory')}/{path.get('filename')}"):
            print("Failed MD5 check!")
        print("MD5 check successful!")

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
            
            # Remove the file from the replica
            try:
                os.remove(path.get("path"))
            except OSError as err:
                print(f"An Error occured during file removal of {path.get('path')}! Error: {err}")
                return False # IDEA: Stop the removal or just continue to the next file?
        
        # Check if the directory exists in source
        if not any(str(a.get("directory")).replace(source, "", 1) == str(path.get("directory")).replace(replica, "", 1) for a in sourceFolders):
            
            # Remove the directory from the replica
            try:
                os.removedirs(f"{replica}/{path.get(f'directory')}")
            except OSError as err:
                print(f"An Error occured during directory removal of {path.get('directory')}! Error: {err}")
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
        print(f"An Error occured during opening of the MD5 hash file of {filePath}! Error: {err}")
        return False
    
    # Update the hasher
    for line in file.readlines():
        hasher.update(line)
    
    # Return the hash
    return hasher.hexdigest()


def md5Check(sourcePath: str, replicaPath: str) -> bool:
    """ This function compares the hashes for each file. """
    
    # Compare each others hashes to determine if 
    # the were clone accurately and not corrupted
    if md5Hasher(sourcePath) != md5Hasher(replicaPath):
        return False
    return True


def process(source: str, replica: str):
    
    # Crawl the source folder
    foundFolders = getFolders(source)
    
    # Check if we found any files
    if len(foundFolders) == 0:
        print("Folder is empty! Nothing to backup.")
        return False
    
    # Create the replica
    print("Creating clones...")
    if not cloneSource(replica, foundFolders):
        print("Failed to create the replica folder properly")
        return False
    print("Successfully cloned!")

    # Remove any files that do not exist
    if not removeOldies(source, replica):
        print("Failed to remove old files.")
        return False
    print("Successfully removed old files.")


if __name__ == "__main__":
    process("source", "replica")
