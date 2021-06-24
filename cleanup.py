from __future__ import print_function
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient import discovery
from pprint import pprint
import time
import random

start = time.time()


###################################################################################################################
# Google API Information
###################################################################################################################

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

scope = [
    "https://spreadsheets.google.com/feeds",
    'https://www.googleapis.com/auth/spreadsheets',
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
    ]

credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(credentials)
service = discovery.build('sheets', 'v4', credentials=ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope))
SPREADSHEET_ID = "1SuAeUZZhKQ67_79u-bueBVqG_B94l6Dz7fGdkQq0m2o"

sheet = client.open("Database").worksheet("Data")
sheet1 = client.open("Database").worksheet("Number_Assigned")
dbData = sheet.get_all_records()
naData = sheet1.get_all_records()


# convert the json to dataframe
records_df = pd.DataFrame.from_dict(dbData)

# view the top records
print(records_df.head())

###################################################################################################################
# Global Variables
###################################################################################################################

brothers = []
cleanupProperties = []
masterDict = {}
staticDict = {}
numberAssigned = {}

###################################################################################################################
# Main Functions
###################################################################################################################



def main():
    read()
    heap()
    write()
    end = time.time()
    print("Your Program took: " +str(end-start)+ " Seconds")    



def read():
    set_brother_names()
    set_cleanups()
    set_master_dict()
    set_number_ssigned()



def heap():
    switch = False
    count = 0
    # loops through all cleanups and creates cleanup list
    while count<len(cleanupProperties):
        if cleanupProperties[count] == "Kitchen":
            switch = True
        if switch:
            cleanup = cleanupProperties[count]
            finalList = select_brothers(cleanup)
            update_local_db(cleanup, finalList)
            remove_names(finalList)
        count=count+1

        #####Testing####

            # print(cleanup)
            # print("Assigned Cleanups" +str(finalList))





def write():
    update_Db()



###################################################################################################################
# Read/database creation Functions
###################################################################################################################



# Dictionary containing a list of lists (kinda like tuples but mutable) for each cleanup
def set_master_dict():
    # Create nested dictionary in the form {cleanup: Name: Personal Data}
    for x in cleanupProperties:
        localList1 = []
        for y in brothers:
            localList2 = []
            localList2.append(y)
            localList2.append(personal_data(x, y))
            localList1.append(localList2)
        masterDict[x] = localList1
        staticDict[x] = localList1
        


# appends the brother array with all brother names in the database
def set_brother_names():
    for x in range(len(dbData)):
        brothers.append(dbData[x]["Name"])   



# appends the otherInfo and cleanupNames array
def set_cleanups():
    for x in dbData[0].keys():
        if x != "" and x != "Name":
            cleanupProperties.append(x)



# appends the Number Assigned dict which holds the number of brothers each cleanup will be assigned
def set_number_ssigned():
    for x in range(len(naData)):
        numberAssigned[naData[x]['Cleanup']] = naData[x]["Number"]



# pass cleanup and name and return val
def personal_data(cleanup, name):
    for x in range(len(dbData)):
        if dbData[x]['Name'] == name:
            return dbData[x][cleanup]



# find index of a brother name in the master dict
def index_of(cleanup, name):
    for x in range(len(masterDict[cleanup])):
        if masterDict[cleanup][x][0] == name:
            return x 



###################################################################################################################
# Randomiser Functions
###################################################################################################################



# takes list of potential brothers for the cleanup and cuts off the unused brothers
def randomizer(numberAssigned, pBrothersList):
    distinct = -1
    bLength = len(pBrothersList)

    # find number of distinct values
    for x in range(bLength):
        if distinct == -1:
            distinct = distinct+2
        elif pBrothersList[x][1] != pBrothersList[x-1][1]:
            distinct = distinct + 1

    # if the list holds the perfect amount of brothers, return that list
    if bLength == numberAssigned:
        return pBrothersList

    # if the list holds more than the perfect amount and all values are equal, 
    # randomly remove loose ends
    elif (bLength > numberAssigned) and distinct == 1:
        while len(pBrothersList) > numberAssigned:    
            del pBrothersList[random.randint(0,len(pBrothersList)-1)]
        return pBrothersList
    # otherwise, del values off the end
    else:
        while len(pBrothersList) > numberAssigned:
            del pBrothersList[-1]
        return pBrothersList



# randomly selects a captain from the list, if there is no elegable brother
# the first brother in the list is captain
def captainSelect(finalList):
    # loops 5 times to in case a bad captain is selected
    for x in range(5):
        randidx = random.randint(0, len(finalList)-1)
        tempIdx = 0
        # Checks to see if a brother is eligble for the captain role
        for y in range(len(masterDict["Captain"])):
            if ((masterDict["Captain"][y][0] == finalList[randidx][0]) 
            and 
            (masterDict["Captain"][y][1]=="Y")):
                return randidx
    # If no selected brother is eligable then the first/only brother is selected
    return 0



###################################################################################################################
# Write/Database Update Functions
###################################################################################################################



def update_Db():
    vrange = {"dog": 5, "Cat": 6}
    request = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range="A1", valueInputOption='USER_ENTERED', body=vrange)
    response = request.execute()
    print(response)
    

    

def update_local_db(cleanup, finalList):
    for x in finalList:
        for y in dbData:
            if y["Name"] == x[0]:
                y["Last"] = cleanup
                y[cleanup] = x[1]+1



def remove_names(finalList):
    switch = False
    for x in cleanupProperties:
        if x == "Kitchen":
            switch = True
        if switch:
            for y in finalList:
                for z in masterDict[x]:
                    if y[0] == z[0]:
                        idx = index_of(x, z[0])
                        del masterDict[x][idx]
                    
    

###################################################################################################################
# Min Heap implementation and Sorting functions
###################################################################################################################



#  cleanup assignments list
def select_brothers(cleanup):
    cleanSort = Min_Heap()
    cleanupAssignments = []
    townsmen = 0

   # adds all memberst of mutable tuple to the Heap
    for x in masterDict[cleanup]:  
        cleanSort.add(x)
    
    while True:
        # populate array as long as their last cleanup wasn't the current
        if cleanup == personal_data("Last", cleanSort.peek()[0]):
            cleanSort.pop()
            continue
        # makes sure there is no more than one townsmen per cleanup and they can't be the only brother on that cleanup
        if "T" == personal_data("Deck", cleanSort.peek()[0]):
            townsmen = townsmen+1          
            if numberAssigned[cleanup] == 1 or townsmen > 1:
                cleanSort.pop()
                continue
        cleanupAssignments.append(cleanSort.pop())

        # break loop if the list is the size of the desired cleanp size and all edge duplicates are accounted for
        if len(cleanupAssignments) >= numberAssigned[cleanup] and cleanupAssignments[-1][1] != cleanSort.peek()[1]:
            break

    # Make random selection
    return randomizer(numberAssigned[cleanup], cleanupAssignments)



# Implement min heap
class Min_Heap:
    def __init__(self, key=lambda x:-x):
        self.data = []
        self.key = key

    @staticmethod
    def _parent(idx):
        return (idx-1)//2

    @staticmethod
    def _left(idx):
        return (idx*2)+1

    @staticmethod
    def _right(idx):
        return (idx*2)+2

    # reformats the heap
    def heapify(self, idx=0):
        temp_idx = idx
        while True:
            l = Min_Heap._left(temp_idx)
            r = Min_Heap._right(temp_idx)
            # if there is a left child and the left child is greater, set temp index to the left childs index
            if l < len(self.data) and self.key(self.data[temp_idx][1]) < self.key(self.data[l][1]):
                temp_idx = l
            # if there is a right child and the right child is greater, set temp index to right child index
            if r < len(self.data) and self.key(self.data[temp_idx][1]) < self.key(self.data[r][1]):               
                temp_idx = r
            # the number is in the correct spot
            if temp_idx == idx:
                break
            else:
                 # swap the values at the temp index and index
                self.data[temp_idx], self.data[idx] = self.data[idx], self.data[temp_idx]
                # reset the value of idx to prep for next iteration
                idx = temp_idx


    def peek(self):
        return self.data[0]

    def add(self, lst):
        
        self.data.append(lst)
        # adds val to the last spot in the array list
        v = len(self.data)-1
        # parent of the added val
        p = Min_Heap._parent(v)

        # while there is more than one val in the list, and the val is not the parent node, and the parent          of val is less than val
        while v > 0 and self.key(self.data[p][1]) < self.key(self.data[v][1]):
            # switch the index of the val and parent
             self.data[p], self.data[v] = self.data[v], self.data[p]
            #  set values to continue up the tree
             v = p
             p = Min_Heap._parent(v)


    def pop(self):
        # save value at the top of the heap
        ret = self.data[0]
        # set the value at the top of the heap to the last val
        self.data[0] = self.data[len(self.data)-1]
        # delete last val
        del self.data[len(self.data)-1]
        # 'trickle down' the heap
        self.heapify()

        return ret

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return repr(self.data)

    def __bool__(self):
        return len(self.data) > 0

main()
