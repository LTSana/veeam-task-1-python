## Veeam Task 1
## Implement a program that synchronizes two folders: source and replica. 
## The program should maintain a full, identical copy of source folder at replica folder.
##
## First we will crawl the source folder and find subfolders storing.
## Second we can start cloning the files in folder to replica.
## Third we will perform a MD5 check on source folder and replica folder.

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
                    "directory": root, # The directory the file is in
                })

    return result
    

if __name__ == "__main__":
    
    # Crawl the source folder
    foundFolders = getFolders("source")
    print(f"Found Folders: {foundFolders}")
    
