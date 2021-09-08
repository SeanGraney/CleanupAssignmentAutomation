from __future__ import print_function
import gspread
import math
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
from datetime import date
import calendar
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

credentials = ServiceAccountCredentials.from_json_keyfile_name(".keys\credentials.json", scope)
client = gspread.authorize(credentials)
service = discovery.build('sheets', 'v4', credentials=ServiceAccountCredentials.from_json_keyfile_name(".keys\credentials.json", scope))

SPREADSHEET_ID = "1SuAeUZZhKQ67_79u-bueBVqG_B94l6Dz7fGdkQq0m2o"
SPREADSHEET_ID1 = "1BRelsFAXI6h3lJrX9LnLx7ttOmZmsO8OEDJ53IgGEzk"

ASSIGNMENTSHEET_ID = "1828663692"

Database = client.open("Database").worksheet("Data")
NumAssigned = client.open("Database").worksheet("Number_Assigned")
Assignments = client.open("Assignments").worksheet("Blank")

dbData = Database.get_all_records()
naData = NumAssigned.get_all_records()


# convert the json to dataframe
records_df = pd.DataFrame.from_dict(dbData)



###################################################################################################################
# Global Variables
###################################################################################################################

brothers = []
cleanupProperties = []
masterDict = {}
staticDict = {}
numberAssigned = {}
townsmenEligibilty = {}
townsmenNums = {}
finalCleanupAssignments = {}

###################################################################################################################
# Main FunctionsS
###################################################################################################################



def main():
    # view the top records
    # print(records_df.head())

    read()
    heap()
    write()
    end = time.time()
    print("Your Program took: " +str(end-start)+ " Seconds")    



def read():
    set_brother_names()
    set_cleanups()
    set_master_dict()
    set_number_assigned()
    set_townsmen_elig()
    set_townsmen_nums()



def heap():
    switch = False
    count = 0
    length = len(cleanupProperties)
    # loops through all cleanups and creates cleanup list
    while count<length:
        if cleanupProperties[count] == "Kitchen":
            switch = True
        if switch:
            print("")
            cleanup = cleanupProperties[count]

            # Testing cleanup list
            # print("" +str(cleanupProperties))
            # print("count " +str(count))


            print("------------------------" +cleanup+ "-----------------------------")
            finalList = select_brothers(cleanup)
            update_local_db(cleanup, finalList)
            remove_names(finalList)

            # make list of names for each assigned cleanup to print to assignments page
            tempList = []
            for x in finalList:
                tempList.append(x[0])

            finalCleanupAssignments[cleanup]=tempList
    
            ####Testing####

            print("Assigned Cleanups: " +str(finalList))
            print("")

        count=count+1
       



def write():
    update_Db()
    captainSelect()
    create_assignment_sheet()



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
def set_number_assigned():
    for x in range(len(naData)):
        numberAssigned[naData[x]['Cleanup']] = naData[x]["Number"]



# set the townsmen eligability dict {cleanup: Y/N, ...}
def set_townsmen_elig():
    for x in range(len(naData)):
        townsmenEligibilty[naData[x]["Cleanup"]] = naData[x]["Townsmen Eligible"]



def set_townsmen_nums():
    for x in range(len(naData)):
        townsmenNums[naData[x]["Cleanup"]] = naData[x]["Number Of Townsmen"]



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
def randomizer(num, pBrothersList):
    distinct = -1
    bLength = len(pBrothersList)

    # find number of distinct values
    for x in range(bLength):
        if distinct == -1:
            distinct = distinct+2
        elif pBrothersList[x][1] != pBrothersList[x-1][1]:
            distinct = distinct + 1

    # if the list holds the perfect amount of brothers, return that list
    if bLength == num:
        return pBrothersList

    # if the list holds more than the perfect amount and all values are equal, 
    # randomly remove loose ends
    elif (bLength > num) and distinct == 1:
        while len(pBrothersList) > num:
            rand = random.randint(0,len(pBrothersList)-1)
            del pBrothersList[rand]
        return pBrothersList

    # otherwise, del values off the end
    else:
        while len(pBrothersList) > num:
            del pBrothersList[-1]
        return pBrothersList



# randomly selects a captain from the list, if there is no elegable brother
# the first brother in the list is captain
def captainSelect():
    for x in numberAssigned.keys():
        names = finalCleanupAssignments[x]
        if numberAssigned[x]>2:
            random.shuffle(names)
            while personal_data('Captain', names[0]) != "Y":
                random.shuffle(names)
        names[0] = "**"+names[0]+"**"





###################################################################################################################
# Write/Database Update Functions
###################################################################################################################


# updates datebase with new values
def update_Db():
    
    # cell values to update
    values = []

    # populates values list with new data and appends it
    for x in dbData:
        tempList = []
        for y in x.items():
            tempList.append(y[1])
        values.append(tempList)

    # set data range
    data = [
        {
            "range": "A2",
            "values": values
        }
    ]
    
    # base info
    body = {
        "valueInputOption": "USER_ENTERED",
        'data': data
    }

    # request to google to fill the sheets
    request = service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
   
    # print('{0} cells updated.'.format(request.get("totalUpdatedCells"))) --- Testing


# Find the date of the upcoming sunday for the title of the next assignments sheet
def find_next_sunday():
    year = date.today().year
    day = date.today().day
    month = date.today().month
    cal = calendar.TextCalendar(calendar.SUNDAY)
    sundays = []
    upcomingMonth = 0
    upcomingDay = 0

    # add all sunday dates of the current year to the array
    for x in range(1,13):
        for y in cal.itermonthdays(year,x):
            if y != 0:
                days = date(year,x,y)
                if days.weekday()==6:
                    sun = [x, y]
                    sundays.append(sun)

    # find the sunday that follows the current date
    for x in sundays:
        if ((x[0] >= month) and (x[1] >= day)):
            upcomingMonth = x[0]
            upcomingDay = x[1]
            break

    ret = (str(upcomingMonth)+"/"+str(upcomingDay))
    return ret


# get cell range to be updated in assignment sheet
def assignment_sheet_ranges():
    ranges = {}
    start = 2
    end = 0
    
    for x in numberAssigned.items():
        start = start + 9
        end = start + int(x[1])-1
        ranges[x[0]]=("B"+str(start)+":B"+str(end))
        start = end

    return ranges




# update assignment sheet with correct names
def update_assignment_sheet(cellRange, nextSun, namesAssigned):

    cell_range_insert = cellRange
    # values to be added to sheet
    vals = (
        namesAssigned,
    )
    value_range_body = {
        'majorDimension': 'COLUMNS',
        'values': vals
    }

    # google api request
    service.spreadsheets().values().update(
        spreadsheetId= SPREADSHEET_ID1,
        valueInputOption= 'USER_ENTERED',
        range=nextSun +"!"+ cell_range_insert,
        body=value_range_body
    ).execute()



def create_assignment_sheet():
    # finds date of next sunday
    nextSun = find_next_sunday()

    # duplicates new sheet
    requests = [
        {
            "addSheet": {
                "properties": {
                    "title": nextSun,
                    "gridProperties": {
                        "rowCount": 124,
                        "columnCount": 11
                    },
                    "tabColor": {
                        "red": 1.0,
                        "green": 0.5,
                        "blue": 0.8
                    }
                }
            }
        }
    ]

    body = {
        "requests": requests
    }

    request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID1, body=body).execute()      

    newSheetId = request["replies"][0]["addSheet"]["properties"]["sheetId"]

    # copy formatting from base sheet to new sheet
    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {"sheetId": 0},
                    "destination": {"sheetId": newSheetId},
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL"
                    },
                # "updateSheetProperties": {
                #     "properties": {
                #         "gridProperties": {
                #             "frozenRowCount": 1
                #         }
                    # }
                # }
            }
        ]
    }


    request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID1, body=body)
    response = request.execute()
    

    # get ranges for assignment sheet
    ranges = assignment_sheet_ranges()

    # update assignment sheet cells for each cleanup
    for x in numberAssigned.keys():
        update_assignment_sheet(ranges[x], nextSun, tuple(finalCleanupAssignments[x]))



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



def populate_final_list(cleanup, num, heap):
    ret = []

    while len(heap)>0:
        # Removes brothers who were on that clenaup the previous week
        if cleanup == personal_data("Last", heap.peek()[0]):
            heap.pop()
            continue

        # Brothers only cleanup their bathrooms respectively
        if ((cleanup == "Bathroom 2") or (cleanup == "Bathroom 3")):
            deck = str(personal_data("Deck", heap.peek()[0]))

            if ((deck != cleanup[-1]) and (deck != "T")):
                heap.pop()
                continue

        # adds brothers to new list
        ret.append(heap.pop())

        # break if the len is greater than desired number and the value is unique
        if len(ret) >= num and ret[-1][1] == heap.peek()[1]:
            break
    
    ret = randomizer(num, ret)

    return ret



#  cleanup assignments list
def select_brothers(cleanup):
    finalList = []

    inhouse = Min_Heap()
    outhouse = Min_Heap()
    townsmen = []
    housemen = []

    numHousemen = 0
    numTownsmen = 0

    shuffledMasterDict = masterDict[cleanup]
    random.shuffle(shuffledMasterDict)
   # adds all members of mutable tuple to the Heap
    for x in shuffledMasterDict: 
        if personal_data("Deck", x[0]) == "T":
            if townsmenEligibilty[cleanup] == "N":
                continue
            outhouse.add(x)
        else:
            inhouse.add(x)

    # decide how many townsmen and inhouse for that cleanup
    if len(outhouse) > 0:
        numTownsmen = townsmenNums[cleanup]
    numHousemen = numberAssigned[cleanup]-numTownsmen

    print("numHousemen: " + str(numHousemen))
    print("numTownsmen: " +str(numTownsmen)) 
    print("")

    # creates list  inhouse brothers, townsmen for the cleanup and combines them
    housemen = populate_final_list(cleanup, numHousemen, inhouse)
    townsmen = populate_final_list(cleanup, numTownsmen, outhouse)
    finalList = housemen + townsmen

    print("Inhouse: " +str(housemen))
    print("Townsmen: " +str(townsmen))
    print("")

    # print("FinalList: " + str(finalList))

    return finalList



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
        if len(self.data) == 0:
            return "empty"
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
