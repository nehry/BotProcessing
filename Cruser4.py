import pygsheets
import time
import csv
import os
import gspread
import sys
from pathlib import Path
from datetime import datetime

# This program is aimed to automate the proccessing of BOT files once Phase 1 is finished.

#Empty lists:
DataList = []
DataList2 = []
InstList = []

#Google Sheet Automation:
# PygSheets
scope = ['https://www.googleapis.com/auth/spreadsheets,' 'https://www.googleapis.com/auth/drive.file',
         'https://www.googleapis.com/auth/drive']
service_file = #(Insert JSON Google API FILE here)
gc = pygsheets.authorize(service_file=service_file)
Worksheet = gc.open("Macquarie Doc Prep Pipeline")
Worksheet2 = gc.open("Bot Processing")
Bot_Review_Sheet = Worksheet.worksheet_by_title("Bot Review")
Import = Worksheet2.worksheet_by_title("Step 1. Import")
Results = Worksheet2.worksheet_by_title("Step 2. Phase 1 Results")
Status = Worksheet.worksheet_by_title("Status")
    # GSpread
creds = gspread.service_account(filename=r"C:\Users\LEGUser\Desktop\Projects\cryptotracker-327411-b7e2a6da147f.json")
Results2 = creds.open("Bot Processing").worksheet("Step 2. Phase 1 Results")
Bot_Review_Sheet2 = creds.open("Macquarie Doc Prep Pipeline").worksheet("Bot Review")
Status2 = creds.open("Macquarie Doc Prep Pipeline").worksheet("Status")

def locateNewestcsv():
    # This function locates the Archive Folder for CRUser4 and finds THE MOST RECENT .csv file.
    # Function returns the most recent CSV file.
    paths = [(p.stat().st_mtime, p) for p in Path("Y:\Archive").iterdir() if p.suffix == ".csv"]
    paths = sorted(paths, key=lambda x: x[0], reverse=True)
    last = paths[0][1]
    os.path.getctime(last)
    mtime = datetime.fromtimestamp(os.path.getctime(last)).strftime('%d/%m %H:%M')
    return last, mtime

def readCSV():
    # After returning the most recent csv file, this function is designed to read all the values within the sheet.
    # It then converts all the values within a list format.
    NewestCSV = locateNewestcsv()[0]
    print(NewestCSV)
    with open(NewestCSV, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar='"')
        data = list(reader)
        return(data)

def writeCSVtoGsheet():
    # After reading all the data in the CSV file and converted into list,
    # It will then paste the data onto the bot processing sheet.
    x = Import.get_col(1)
    Import.clear(start='A2', end='AE28')
    for i in range(1,28):
        if len(Import.get_col(1)[i]) == 0:
            Import.update_values('A{}'.format(i+1), readCSV()[1:])
            break
    time.sleep(10)

#After this section, the BOT begins the 2nd half of the program which is extract the the columns from the Phase 1 result worksheet.

def getRowinImport():
    # Extracts the Total Rows of all instructions on the Import spreadsheet.
    # The reason why we want to extract the TOTAL row via LEN is to ensure that no matter the amount of instructions puts in
    # We can get the total amount of instructions exact.

    x = Import.get_col(1)[1:]

    for a in range(1, len(x)):
        if len(x[a]) == 0:
            return a
            break

def getAPPinImport():
    # Using the returned row value from the previous function, we can then extract the APP from the Import Worksheet.
    # Then, we store the APPs in a List and use it to find the rows of the same APP in the results tab.
    # We can then extract the column values in the phase 1 results.
    x = getRowinImport()
    APPList = Import.get_values("A2","A{}".format(x+1))

    for x in APPList:
        try:
            y = ''.join(x)
            cell = Results2.find(y).row
            print(Results.get_values("A{}".format(cell),"M{}".format(cell)))
            DataList.append(Results.get_values("A{}".format(cell),"M{}".format(cell)))
            DataList2.append(Results.get_values("B{}".format(cell), "M{}".format(cell)))
        except:
            print("{} does not appear in the Results Worksheet, Program will now exit.".format(x))
            sys.exit()

    for y in range(len(DataList)):
        InstList.append(DataList[y][0][0])

def findRowBotReview():
    # This code will now find the APP in the Bot_Review sheet and paste in the values from the Results Tab.

    # BELOW IS OLD GENERIC CODE, IT WORKS BUT RUNS VERY VERY SLOW.
    # for x in InstList:
    #     cell = Bot_Review_Sheet.find(x)[0].row
    #     print(cell)
    mtime = locateNewestcsv()[1]
    cols = Bot_Review_Sheet2.col_values(4)
    for x in InstList:
        for i in cols:
            if x == i:
                # If code is not picking the correct row, use the below to figure if bot is writing to correct code.
                # print(cols.index(i) + 1)
                for o in range(len(DataList)):
                    if x == DataList[o][0][0]:
                        Bot_Review_Sheet.update_value('E{}'.format(cols.index(i)+1), "BOT 4")
                        Bot_Review_Sheet.update_value('F{}'.format(cols.index(i)+1), mtime)
                        Bot_Review_Sheet.update_values('G{}'.format(cols.index(i)+1), DataList2[o])
                        print("Writing to Row {} with {}".format(cols.index(i)+1, x))

def PendingDocPrep():
    #The final part of this function will now change Column D on the Status Worksheet to Pending Doc Prep.
    cols = Status2.col_values(4)
    for x in InstList:
        for i in cols:
            if x == i:
                    Status.update_value('D{}'.format(cols.index(i)+1), "3a. Pending Doc Prep")
                    print ("Updating Status on row {}".format(cols.index(i+1), x))


locateNewestcsv()
writeCSVtoGsheet()
getAPPinImport()
findRowBotReview()
PendingDocPrep()
