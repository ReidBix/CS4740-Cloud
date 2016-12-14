import pymysql.cursors
import sys

# Connect to the database
connection = pymysql.connect(host='uvaclasses.martyhumphrey.info',
                             user='UVAClasses',
                             password='WR6V2vxjBbqNqbts',
                             db='uvaclasses',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

dept = sys.argv[1]
course = sys.argv[2]

try:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `seatsTaken` FROM `section` WHERE `deptID`=%s AND `courseNum`=%s AND `semester`=1162"
        cursor.execute(sql, (dept,course))
        result = cursor.fetchone()
        seatsTaken = int(result['seatsTaken'])

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `seatsOffered` FROM `section` WHERE `deptID`=%s AND `courseNum`=%s AND `semester`=1162"
        cursor.execute(sql, (dept,course))
        result = cursor.fetchone()
        seatsOffered = int(result['seatsOffered'])

    print(seatsOffered-seatsTaken)
finally:
    connection.close()
