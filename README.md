# veeam-task-1-python
Veeam Task 1 built with Python. Implement a program that synchronizes two folders: source and replica. The program should maintain a full, identical copy of source folder at replica folder.

## Command line arguments as order
The arguments to be passed in the command line should be in this order.
- path to source folder
- path to replica folder
- interval between synchronizations (This is in seconds)
- amount of synchronizations 
- path to log file

Example: `python main.py path/to/source path/to/replica 30 5 path/to/log`
