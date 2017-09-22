import requests
import arcpy
import time
import sys
from datetime import datetime

arcpy.env.overwriteOutput = "True"
workspace = "D:/EndomondoData/Spain/EndoUsers.gdb/"
users_table = "EndoUsers"
userslog_table ="EndoUsers_log"
stats_table = "EndoUsers_Statistics"
users_table_path = workspace + users_table
userslog_table_path = workspace + userslog_table
statsPath = workspace + stats_table
finish_message = ''
# user 29188815 date 6/21/2016
# staring process in user 29000000

# To do:    Define start user ID

retriveIDs = 100000 # defines the number of userID to be requested starting from last requested ID in table
waittime_404 = 1
waittime_429 = 30

# counters starters
count_sessionrequest = 0
count_totalrequests = 0
count404 = 0
count200 = 0

#timers
process_starttime = datetime.now()
session_starttime = datetime.now()

def calculate_statistics(users_table_path, statsPath):
    print("Calculating Statistics...")
    arcpy.Statistics_analysis(in_table=users_table_path, out_table=statsPath, statistics_fields="id MIN", case_field="")
    print("...Done with Statistics")

def get_minID():
    global start_userid
    fields = ['MIN_ID']
    cursor = arcpy.da.SearchCursor(statsPath, fields)
    for row in cursor:
        start_userid = row[0]
    del cursor
    return start_userid

def addusertotable(userID, gender, height, date_of_birth, workout_count, created_date, country, name, is_public):

    user_fields = ['id', 'gender', 'height', 'date_of_birth', 'workout_count', 'created_date', 'country',
                   'name', 'is_public']

    with arcpy.da.InsertCursor(users_table_path, user_fields) as usercursor:
        table_row = [userID, gender, height, date_of_birth, workout_count, created_date, country, name, is_public]
        usercursor.insertRow(table_row)

def logError(response_code, currentID):

    # capture and calculate session times before logging
    global session_starttime
    session_endtime = datetime.now()
    session_time = session_endtime - session_starttime  # time without being blocked
    process_elapsedtime = datetime.now() - process_starttime  # total time running script

    userslog_fields = ['response_code', 'requests_sent', 'session_time', 'code_404', 'code_200',
                       'total_runtime', 'total_requests', 'error_date', 'from_userID', 'to_userID']

    with arcpy.da.InsertCursor(userslog_table_path, userslog_fields) as userlogcursor:

        logrow = [response_code, count_sessionrequest, session_time.seconds, count404, count200,
                  process_elapsedtime.seconds, count_totalrequests, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  session_startuserid, currentID]
        userlogcursor.insertRow(logrow)

    #logged feedback here
    if finish_message == "Job Completed":
        process_elapsedtime = datetime.now() - process_starttime
        print("")
        print("The job finished completely" + " from: " + str(start_userid) + "  to: " + str(currentID) +
              '     total time: ' + str(process_elapsedtime.total_seconds())[:4] + 's')
    else:
        print('')
        print("------------------------ STATUS CODE: " + str(responseuser.status_code) + " ------------------------")
        print("Responses 404: " + str(count404) + "   Session Requests:  " + str(count_sessionrequest)
              + "    Session Time: " + str(session_time.total_seconds())[:4] + "s" + '  startid: ' + str(session_startuserid),
              "    toID: " + str(userID))
        print("waiting " + str(waittime_429) + " seconds to try again...")
        print("")

def response200():
    global count200
    count200 += 1
    user_json = responseuser.json()
    userID = user_json['id']
    gender = user_json['gender']
    try:
        height = user_json['height']
    except KeyError:
        height = None
    try:
        date_of_birth = user_json['date_of_birth']
        date_of_birth = date_of_birth[:-14].replace("T", " ")
    except KeyError:
        date_of_birth = None
    workout_count = user_json['workout_count']
    created_date = user_json['created_date']
    created_date = created_date[:-14].replace("T", " ")
    country = user_json['country']
    name = user_json['name']
    is_public = "True"

    addusertotable(userID, gender, height, date_of_birth, workout_count, created_date,
                   country, name, is_public)

def response404():

    global count404
    count404 += 1
    gender = None
    height = None
    date_of_birth = None
    workout_count = None
    created_date = None
    country = None
    name = None
    is_public = "False"

    addusertotable(userID, gender, height, date_of_birth, workout_count, created_date,
                   country, name, is_public)

# start_userid = 29000000
calculate_statistics(users_table_path, statsPath)
start_userid = int(get_minID()) - 1 # finds where it was left on, use after the first try
end_userid = start_userid - retriveIDs
session_startuserid = start_userid

for userID in range(start_userid, end_userid - 1, -1):

    urluserroot = "https://www.endomondo.com/rest/v1/users/"
    urluserrequest = urluserroot + str(userID)
    responseuser = requests.get(urluserrequest, headers={'User-Agent': 'Chrome/59.0.3071.115'})
    count_sessionrequest += 1
    count_totalrequests += 1

    # response 200
    if responseuser.status_code == 200:   
        response200()

    # count_sessionrequest 404 responses
    elif responseuser.status_code == 404:
        response404()
        time.sleep(waittime_404) #used to reduce 404 responses in time

    # any other server response code needs to be logged
    else:
        logError(responseuser.status_code, userID)

        session_startuserid = userID # starts a new session ID for tracking
        session_starttime = datetime.now() # resets time for new session
        count200 = 0
        count404 = 0
        count_sessionrequest = 0
        time.sleep(waittime_429)

        # try again
        responseuser = requests.get(urluserrequest, headers={'User-Agent': 'Chrome/59.0.3071.115'})
        count_sessionrequest += 1
        count_totalrequests += 1
        print(responseuser.status_code)
        if responseuser.status_code == 200:
            print("the wait worked!")
            print(" ")
            response200()

        elif responseuser.status_code == 404:
            print("the wait worked!")
            print(" ")
            response404()

        else:
            print("stopping script... server still blocking requests")
            logError(responseuser.status_code, userID)
            sys.exit(0)

    process_elapsedtime = datetime.now() - process_starttime

    # feedback users writing
    print("request: " + str(count_sessionrequest) + "   id: " + str(userID) + "   request time: "
          + str(responseuser.elapsed)[6:-4] + "s    total time: "
          + str(int(process_elapsedtime.total_seconds())) + "s   status: " + str(responseuser.status_code))

finish_message = "Job Completed"
logError(finish_message, end_userid)
