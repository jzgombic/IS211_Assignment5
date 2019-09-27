#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argparse
import time
import csv
import os


parser = argparse.ArgumentParser()
parser.add_argument('--file', help = 'The data file to process.')
parser.add_argument('--servers', help = 'The number of servers.')
args = parser.parse_args()


def main():
    ServerCount = args.servers

    if args.file:
        if os.path.isfile(args.file) is True:
            if ServerCount:
                if str(ServerCount).isdigit() is False:
                    print ('Please enter a valid number of servers')
                    print (' ')
                    print ('optional arguments:')
                    print ('   --file        FILE        The data file to process.')
                    print ('   --servers     SERVERS     The number of servers as a whole number.')
                else:
                    if(int(ServerCount) > 1):
                        get_file = simulateManyServers(args.file, int(ServerCount))
                    else:
                        get_file = simulateOneServer(args.file)
            else:
                get_file = simulateOneServer(args.file)
        else:
            print ('Please verify the file name and/or location')
    else:
        print ('Please enter a file name and location')
        print (' ')
        print ('optional arguments:')
        print ('   --file        FILE        The data file to process.')
        print ('   --servers     SERVERS     The number of servers as a whole number.')
        

class Queue:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server:
    def __init__(self):        
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self,new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_length()


class Request:
    def __init__(self, time, length):
        self.timestamp = time
        self.length = int(length)

    def get_stamp(self):
        return self.timestamp

    def get_length(self):
        return self.length

    def wait_time(self, current_time):
        return current_time - self.timestamp


def simulateOneServer(file):
    '''

    Processes content within a .csv file and is responsible for printing out the average wait time for a request.

    Args:
        file (file): A .csv file supplied by user.

    Returns:
        message (str): message = A string containing the average wait time in seconds and the number of tasks remaining for one server.

    '''
    
    server = Server()
    queue = Queue()
    wait = []

    with open(file, 'rb') as csvfile:
        datafile = csv.reader(csvfile, delimiter=',')

        for line in datafile:
            timestamp = int(line[0])
            file = line[1]
            length =  int(line[2])
            time = Request(timestamp, length)
            queue.enqueue(time)

            if(not server.busy()) and (not queue.is_empty()):
                next_request = queue.dequeue()
                wait.append(next_request.wait_time(timestamp))
                server.start_next(time)
                
            server.tick()

    average_wait = sum(wait) / len(wait)

    message = ("Average Wait %6.2f secs %3d tasks remaining." % (average_wait, queue.size()))
    
    print message


def simulateManyServers(file, ServerCount):
    '''

    Processes content within a .csv file for a number of servers and is responsible for printing out the average wait time for a request.

    Args:
        file (file): A .csv file supplied by user.
        ServerCount (int): The number of servers to query

    Returns:
        message (str): message = A string containing the average wait time in seconds and the number of tasks remaining for each server.

    '''
    
    servers = []
    queues = []
    wait = []

    for serverNum in range(0, ServerCount):
        servers.append(Server())

    for serverNum in range(0, ServerCount):
        queues.append(Queue()) 

    for serverNum in range(0, ServerCount):
        wait.append([])

    with open(file, 'rb') as csvfile:
        datafile = csv.reader(csvfile, delimiter=',')

        roundRobinPosition = 0

        for line in datafile:
            timestamp = int(line[0])
            file = line[1]
            length =  int(line[2])
            time = Request(timestamp, length)

            queues[roundRobinPosition].enqueue(time)

            if roundRobinPosition < ServerCount - 1:
                roundRobinPosition += 1
            else:
                roundRobinPosition = 0

            if (not servers[roundRobinPosition].busy()) and (not queues[roundRobinPosition].is_empty()):
                next_request = queues[roundRobinPosition].dequeue()
                wait[roundRobinPosition].append(next_request.wait_time(timestamp))
                servers[roundRobinPosition].start_next(time)
                
            servers[roundRobinPosition].tick()

        for serverNum in range(0, ServerCount):
            average_wait = sum(wait[serverNum]) / len(wait[serverNum])

            message = ("Average Wait %6.2f secs %3d tasks remaining." % (average_wait, queues[serverNum].size())) 

            print message


if __name__ == '__main__':
    main()





























#Author: Johnny Zgombic
#Date: September 27, 2019
