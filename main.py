## Veeam Task 1
## Implement a program that synchronizes two folders: source and replica. 
## The program should maintain a full, identical copy of source folder at replica folder.
##
## 1. Crawl the source folder and find subfolders. (Done)
## 2. Start cloning the files in the source folder to replica. (Done)
## 3. Remove any files that aren't in the source folder anymore.
## 4. Perform a MD5 check on source folder and replica folder.
## 5. Setup a sync interval. (5 minutes?)

import os

def getFolders(sourcePath: str) -> list:
    """ This function is for building a list of all the paths for each file in the source folder. """

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
        
        # Return true to signal that it was successful
        return True
    

def removeOldies(sourcePath: str, replicaPath: str) -> bool:
    # TODO
    return False


def md5Check(sourcePath: str, replicaPath: str) -> bool:
    # TODO
    return False


if __name__ == "__main__":
    
    # Crawl the source folder
    foundFolders = getFolders("source")
    print(f"Found Folders: {foundFolders}")
    
    # Create the replica
    print("Creating clones...")
    if not cloneSource("replica", foundFolders):
        print("Failed to create the replica folder properly")
    else:
        print("Successfully cloned!")
    
