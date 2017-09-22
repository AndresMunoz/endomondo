import requests
import arcpy
from datetime import datetime

#  to do:
#
workspace = "D:/EndomondoData/Spain/EndoUsers.gdb/"
workspace2 = "D:/EndomondoData/Spain/EndoTrails.gdb/"

users_table = "EndoUsers"
trails_FeatureClass = "EndoTrails"
workoutlog_table = "EndoTrails_log"
users_table_path = workspace + users_table              # update cursor
trails_location = workspace2 + trails_FeatureClass      # insert cursor
workoutlog_table_path = workspace2 + workoutlog_table   # insert cursor

# declaring Global Variables
users_to_process = 0
users_left = 0
firstUserID = 0

def delete_usertrails(userID):
    print("deleting trails of user ID: " + str(userID) + "...")

    trailFields = ["id", "sport", "start_time", "local_start_time", "distance", "duration", "speed_avg", "speed_max",
                   "altitude_min", "altitude_max", "ascent", "descent", "calories", "author_id", "author_gender",
                   "author_height", "author_dob", "author_country", "author_name", "is_public", "link", "SHAPE@"]

    WhereExpression = "author_id = " + str(userID)
    countdeletedrows = 0
    with arcpy.da.UpdateCursor(trails_location, trailFields, WhereExpression) as cursorEndoTrails:

        for row in cursorEndoTrails:
            cursorEndoTrails.deleteRow()
            countdeletedrows += 1

    print("...deleted " + str(countdeletedrows) + " rows")


def log_workoutdata(Workout_ID, workoutlog_table_path, response_Time, response_Code, skipped, skipped_reason, user_id):
    logRequestFields = ['Workout_ID', 'response_time', 'response_code', 'request_date', 'skipped', 'skipped_reason', 'user_id']

    with arcpy.da.InsertCursor(workoutlog_table_path, logRequestFields) as cursorlog:
        cursorlogRow = [Workout_ID, response_Time, response_Code, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), skipped,
                        skipped_reason, user_id]
        cursorlog.insertRow(cursorlogRow)


def get_traildata(currentID, workoutID, gender, height, date_of_birth, country, current_workout, count_workouts):

    urlroot = "https://www.endomondo.com/rest/v1/users/"
    url = urlroot + str(currentID) + "/workouts/" + str(workoutID)
    print(url + "   " + str(current_workout + 1) + " of " + str(count_workouts))
    response = requests.get(url, headers={'User-Agent': 'Chrome/59.0.3071.115'})
    response_Time = response.elapsed.microseconds * 0.000001
    response_Code = response.status_code
    parsed_json = response.json()

    workout_id = parsed_json["id"]
    sport = parsed_json["sport"]
    start_time = parsed_json["start_time"]
    start_time = start_time[:-1].replace("T", " ")[:-4]
    local_start_time = parsed_json["local_start_time"]
    local_start_time = local_start_time[23:]

    try:
        distance = parsed_json["distance"]
    except KeyError:
        distance = 0

    if distance > 0:
        try:
            duration = parsed_json["duration"]
        except KeyError:
            duration = None
        try:
            speed_avg = parsed_json["speed_avg"]
        except KeyError:
            speed_avg = None
        try:
            speed_max = parsed_json["speed_max"]
        except KeyError:
            speed_max = None
        try:
            altitude_min = parsed_json["altitude_min"]
        except KeyError:
            altitude_min = None
        try:
            altitude_max = parsed_json["altitude_max"]
        except KeyError:
            altitude_max = None
        try:
            ascent = parsed_json["ascent"]
        except KeyError:
            ascent = None
        try:
            descent = parsed_json["descent"]
        except KeyError:
            descent = None
        try:
            calories = parsed_json["calories"]
        except KeyError:
            calories = None
        author_id = parsed_json["author"]["id"]
        author_gender = gender
        author_height = height
        author_dob = date_of_birth
        author_country = country
        author_name = parsed_json["author"]["name"]
        is_public = "True"
        link = parsed_json["link"]

        points = arcpy.Array()

        try:
            for x in parsed_json["points"]["points"]:
                try:
                    lon = x['longitude']
                    lat = x['latitude']
                    try:
                        z = x['altitude']
                    except:
                        z = 0

                    m = x['duration'] / 10000
                    point = arcpy.Point(lon, lat, z, m)
                except KeyError:
                    print('point with error')
                else:
                    points.append(point)

            sr = arcpy.SpatialReference(104016)
            esriShapePolyline = arcpy.Polyline(points, sr, True, True)

        except:
            print("could not load geometry...skipped")
            skipped = "True"
            skipped_reason = "Geometry Error, Missing Coordinates"
            log_workoutdata(workoutID, workoutlog_table_path, response_Time, response_Code, skipped,
                            skipped_reason, currentID)

        else:
            trailFields = ["id", "sport", "start_time", "local_start_time", "distance", "duration", "speed_avg", "speed_max",
                           "altitude_min", "altitude_max", "ascent", "descent", "calories", "author_id", "author_gender",
                           "author_height", "author_dob", "author_country", "author_name", "is_public", "link", "SHAPE@"]

            with arcpy.da.InsertCursor(trails_location, trailFields) as cursor:

                cursor_row = [workout_id, sport, start_time, local_start_time, distance, duration, speed_avg, speed_max, altitude_min,
                              altitude_max, ascent, descent, calories, author_id, author_gender, author_height, author_dob,
                              author_country, author_name, is_public, link,  esriShapePolyline]

                cursor.insertRow(cursor_row)

            skipped = "False"
            skipped_reason = "Loaded Correctly"
            log_workoutdata(workoutID, workoutlog_table_path, response_Time, response_Code, skipped,
                            skipped_reason, currentID)

    else:
        print("workout: " + str(workout_id) + " to short...skiped.")
        skipped = "True"
        skipped_reason = "Trail to short"
        log_workoutdata(workoutID, workoutlog_table_path, response_Time, response_Code, skipped,
                        skipped_reason, currentID)


def getuser_workouts(userID, gender, height, date_of_birth, created_date, country):

    url_root_workouts = "https://www.endomondo.com/rest/v1/users/"
    start_date = created_date
    end_date = datetime.now().strftime('%Y-%m-%d')
    url_workouts = url_root_workouts + str(userID) + "/workouts?before=" + end_date + "&after=" + str(start_date)[:10]
    print("left " + str(users_left) + " out of " + str(users_to_process) + " users to process ")
    print(url_workouts)
    response = requests.get(url_workouts, headers={'User-Agent': 'Chrome/59.0.3071.115'})
    parsed_json = response.json()
    count_workouts = len(parsed_json)

    for current_workout in range(0, count_workouts):
        workoutID = parsed_json[current_workout]['id']
        get_traildata(userID, workoutID, gender, height, date_of_birth, country, current_workout, count_workouts)


def getID(users_table_path):
    global users_to_process
    global users_left

    user_fields = ['id', 'gender', 'height', 'date_of_birth', 'workout_count', 'created_date', 'country',
                   'name', 'is_public', 'workouts_downloaded_on']
    WhereExpression = "is_public = 'True' AND country = 'ES' AND workout_count > 0 AND workouts_downloaded_on IS NULL"

    # count records to process and get first userID on the list
    users_to_process = 0
    global firstUserID

    with arcpy.da.SearchCursor(users_table_path, user_fields, WhereExpression) as cursor:

        for row in cursor:
            if users_to_process == 0:
                firstUserID = row[0]

            users_to_process += 1

    users_left = users_to_process
    delete_usertrails(firstUserID)
    print("Records to be processed:  " + str(users_to_process))



    # start reading userIDs to retrieve their workouts
    with arcpy.da.UpdateCursor(users_table_path, user_fields, WhereExpression) as cursorEndoUsers:

        for row in cursorEndoUsers:
            userID = row[0]
            gender = row[1]
            height = row[2]
            date_of_birth = row[3]
            created_date = row[5]
            country = row[6]
            getuser_workouts(userID, gender, height, date_of_birth, created_date, country)
            users_left -= 1
            row[9] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursorEndoUsers.updateRow(row)


getID(users_table_path) # runs the whole process