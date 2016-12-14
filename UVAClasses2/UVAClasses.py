import urllib2
import json
import pymysql.cursors
import sys


API_BASE="http://bartjsonapi.elasticbeanstalk.com/api"

def lambda_handler(event, context):
    if (event["session"]["application"]["applicationId"] !=
            "amzn1.ask.skill.0eca8550-e80c-45eb-a13d-d60c953581e8"):
        raise ValueError("Invalid Application ID")
    
    if event["session"]["new"]:
        on_session_started({"requestId": event["request"]["requestId"]}, event["session"])

    if event["request"]["type"] == "LaunchRequest":
        return on_launch(event["request"], event["session"])
    elif event["request"]["type"] == "IntentRequest":
        return on_intent(event["request"], event["session"])
    elif event["request"]["type"] == "SessionEndedRequest":
        return on_session_ended(event["request"], event["session"])

def on_session_started(session_started_request, session):
    print "Starting new session."

def on_launch(launch_request, session):
    return get_welcome_response()

def on_intent(intent_request, session):
    intent = intent_request["intent"]
    intent_name = intent_request["intent"]["name"]

    if intent_name == "GetStatus":
        return get_system_status()
    elif intent_name == "GetElevators":
        return get_elevator_status()
    elif intent_name == "GetTrainTimes":
        return get_train_times(intent)
    elif intent_name == "WhenMeet":
        return when_meet(intent)
    elif intent_name == "WhereMeet":
        return where_meet(intent)
    elif intent_name == "HowMany":
        return how_many(intent)
    elif intent_name == "AMAZON.HelpIntent":
        return get_welcome_response()
    elif intent_name == "AMAZON.CancelIntent" or intent_name == "AMAZON.StopIntent":
        return handle_session_end_request()
    else:
        raise ValueError("Invalid intent")

def on_session_ended(session_ended_request, session):
    print "Ending session."
    # Cleanup goes here...

def handle_session_end_request():
    card_title = "BART - Thanks"
    speech_output = "Thank you for using the BART skill.  See you next time!"
    should_end_session = True

    return build_response({}, build_speechlet_response(card_title, speech_output, None, should_end_session))

def get_welcome_response():
    session_attributes = {}
    card_title = "BART"
    speech_output = "Welcome to the Alexa BART times skill. " \
                    "You can ask me for train times from any station, or " \
                    "ask me for system status or elevator status reports."
    reprompt_text = "Please ask me for trains times from a station, " \
                    "for example Fremont."
    should_end_session = False
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def get_system_status():
    session_attributes = {}
    card_title = "BART System Status"
    reprompt_text = ""
    should_end_session = False

    response = urllib2.urlopen(API_BASE + "/status")
    bart_system_status = json.load(response)   

    speech_output = "There are currently " + bart_system_status["traincount"] + " trains operating. "

    if len(bart_system_status["message"]) > 0:
        speech_output += bart_system_status["message"]
    else:
        speech_output += "The trains are running normally."

    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def get_elevator_status():
    session_attributes = {}
    card_title = "BART Elevator Status"
    reprompt_text = ""
    should_end_session = False

    response = urllib2.urlopen(API_BASE + "/elevatorstatus")
    bart_elevator_status = json.load(response) 

    speech_output = "BART elevator status. " + bart_elevator_status["bsa"]["description"]

    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def when_meet(intent):
    # Connect to the database
    connection = pymysql.connect(host='uvaclasses.martyhumphrey.info',
                             user='UVAClasses',
                             password='WR6V2vxjBbqNqbts',
                             db='uvaclasses',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    session_attributes = {}
    card_title = "When Class Meets"
    speech_output = "I'm not sure which class you wanted times for. " \
                    "Please try again."
    reprompt_text = "I'm not sure which class you wanted times for. " \
                    "Try asking about CS 1110 or CS 4740 for example."
    should_end_session = False

    if "Class" in intent["slots"]:
        class_name = intent["slots"]["Class"]["value"]
        dept, course = class_name.split(' ');
        course = course.replace(',','')
        
        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT `Days` FROM `CS1172Data` WHERE `Mnemonic`=%s AND `Number`=%s"
                cursor.execute(sql, (dept,course))
                result = cursor.fetchone()
                speech_output = class_name + " meets at " + result['Days']
                reprompt_text = ""
        finally:
            connection.close()
        
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))
    
def where_meet(intent):
    # Connect to the database
    connection = pymysql.connect(host='uvaclasses.martyhumphrey.info',
                             user='UVAClasses',
                             password='WR6V2vxjBbqNqbts',
                             db='uvaclasses',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    session_attributes = {}
    card_title = "Where Class Meets"
    speech_output = "I'm not sure which class you wanted locations for. " \
                    "Please try again."
    reprompt_text = "I'm not sure which class you wanted locations for. " \
                    "Try asking about CS 1110 or CS 4740 for example."
    should_end_session = False

    if "Class" in intent["slots"]:
        class_name = intent["slots"]["Class"]["value"]
        dept, course = class_name.split(' ');
        course = course.replace(',','')
        
        
        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT `Room` FROM `CS1172Data` WHERE `Mnemonic`=%s AND `Number`=%s"
                cursor.execute(sql, (dept,course))
                result = cursor.fetchone()
                speech_output = class_name + " meets at the location " + result['Room']
                reprompt_text = ""
        finally:
            connection.close()
            
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))
        
def how_many(intent):
    # Connect to the database
    connection = pymysql.connect(host='uvaclasses.martyhumphrey.info',
                             user='UVAClasses',
                             password='WR6V2vxjBbqNqbts',
                             db='uvaclasses',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    session_attributes = {}
    card_title = "How Many in Class"
    speech_output = "I'm not sure which class you wanted seat numbers for. " \
                    "Please try again."
    reprompt_text = "I'm not sure which class you wanted seat numbers for. " \
                    "Try asking about CS 1110 or CS 4740 for example."
    should_end_session = False

    if "Class" in intent["slots"]:
        class_name = intent["slots"]["Class"]["value"]
        dept, course = class_name.split(' ');
        course = course.replace(',','')
        
        
        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT `Enrollment` FROM `CS1172Data` WHERE `Mnemonic`=%s AND `Number`=%s"
                cursor.execute(sql, (dept,course))
                result = cursor.fetchone()
                seatsTaken = int(result['Enrollment'])
        
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT `EnrollmentLimit` FROM `CS1172Data` WHERE `Mnemonic`=%s AND `Number`=%s"
                cursor.execute(sql, (dept,course))
                result = cursor.fetchone()
                seatsOffered = int(result['EnrollmentLimit'])
        
            res = seatsOffered-seatsTaken
            speech_output = class_name + " has " + str(res) + " seats left"
            reprompt_text = ""
        finally:
            connection.close()
            
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def get_train_times(intent):
    session_attributes = {}
    card_title = "BART Departures"
    speech_output = "I'm not sure which station you wanted train times for. " \
                    "Please try again."
    reprompt_text = "I'm not sure which station you wanted train times for. " \
                    "Try asking about Fremont or Powell Street for example."
    should_end_session = False

    if "Station" in intent["slots"]:
        station_name = intent["slots"]["Station"]["value"]
        station_code = get_station_code(station_name.lower())

        if (station_code != "unkn"):
            card_title = "BART Departures from " + station_name.title()

            response = urllib2.urlopen(API_BASE + "/departures/" + station_code)
            station_departures = json.load(response)   

            speech_output = "Train departures from " + station_name + " are as follows: "
            for destination in station_departures["etd"]:
                speech_output += "Towards " + destination["destination"] + " on platform " + destination["estimate"][0]["platform"] + ". ";
                for estimate in destination["estimate"]:
                    if estimate["minutes"] == "Leaving":
                        speech_output += "Leaving now: "
                    elif estimate["minutes"] == "1":
                        speech_output += "In one minute: "
                    else:
                        speech_output += "In " + estimate["minutes"] + " minutes: "

                    speech_output += estimate["length"] + " car train. "

            reprompt_text = ""

    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def get_station_code(station_name):
    return {
        "12th street oakland city center": "12th",
        "16th street mission": "16th",
        "19th street oakland": "19th",
        "24th street mission": "24th",
        "ashby": "ashb",
        "balboa park": "balb",
        "bay fair": "bayf",
        "castro valley": "cast",
        "civic center": "civc",
        "coliseum": "cols",
        "colma": "colm",
        "concord": "conc",
        "daly city": "daly",
        "downtown berkeley": "dbrk",
        "dublin pleasanton": "dubl",
        "el cerrito del norte": "deln",
        "del norte": "deln",
        "el cerrito plaza": "plza",
        "embarcadero": "embr",
        "fremont": "frmt",
        "fruitvale": "ftvl",
        "glen park": "glen",
        "hayward": "hayw",
        "lafayette": "lafy",
        "lake merritt": "lake",
        "macarthur": "mcar",
        "millbrae": "mlbr",
        "montgomery street": "mont",
        "north berkeley": "nbrk",
        "north concord martinez": "ncon",
        "oakland airport": "oakl",
        "orinda": "orin",
        "pittsburg bay point": "pitt",
        "pleasant hill": "phil",
        "powell street": "powl",
        "richmond": "rich",
        "rockridge": "rock",
        "san bruno": "sbrn",
        "san francisco airport": "sfia",
        "san leandro": "sanl",
        "south hayward": "shay",
        "south san francisco": "ssan",
        "union city": "ucty",
        "walnut creek": "wcrk",
        "west dublin pleasanton": "wdub",
        "west oakland": "woak",
    }.get(station_name, "unkn")

def build_speechlet_response(title, output, reprompt_text, should_end_session):
    return {
        "outputSpeech": {
            "type": "PlainText",
            "text": output
        },
        "card": {
            "type": "Simple",
            "title": title,
            "content": output
        },
        "reprompt": {
            "outputSpeech": {
                "type": "PlainText",
                "text": reprompt_text
            }
        },
        "shouldEndSession": should_end_session
    }

def build_response(session_attributes, speechlet_response):
    return {
        "version": "1.0",
        "sessionAttributes": session_attributes,
        "response": speechlet_response
    }

