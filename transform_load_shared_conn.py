import sqlite3
from lxml import etree
import logging
from datetime import datetime

NS = {'siri': 'http://www.siri.org.uk/siri'}


def parse_xml(source, source_type='string'):
    """
    Parses XML data from a given source, which can be either a file path or a direct XML string.

    :param source: The XML source, either a file path or an XML string.
    :param source_type: The type of the source, 'file' for file paths or 'string' for XML strings.
    :return: A list of data extracted from the XML.
    """
    NS = {'siri': 'http://www.siri.org.uk/siri'}  # Namespace declaration

    # Parse the XML based on the source type
    if source_type == 'file':
        # For file source, open the file and parse it
        with open(source, 'rb') as f:
            tree = etree.parse(f)
    elif source_type == 'string':
        # For string source, parse directly from the string
        tree = etree.fromstring(source)
    else:
        raise ValueError("Unsupported source_type. Use 'file' or 'string'.")

    root = tree

    # Extract the ResponseTimestamp from the ServiceDelivery element
    # Ensure to navigate correctly considering the namespace
    service_delivery_timestamp = root.find('.//siri:ServiceDelivery/siri:ResponseTimestamp', NS).text

    # Convert service_delivery_timestamp to the desired format if necessary
    # For example, if you need it as a datetime object or a specific string format,
    # you can convert it here using datetime.strptime() and then format it as needed.

    data = []
    for vehicle_activity in root.findall('.//siri:VehicleActivity', NS):
        # Now pass the extracted service_delivery_timestamp to extract_data
        extracted_data = extract_data(vehicle_activity, service_delivery_timestamp)
        if extracted_data:  # Ensure extracted_data is not None
            data.append(extracted_data)
    return data

def extract_data(vehicle_activity, service_delivery_timestamp):
    NS = {'siri': 'http://www.siri.org.uk/siri'}
    recorded_at_time = vehicle_activity.find('.//siri:RecordedAtTime', NS).text
    date_of_journey = datetime.strptime(recorded_at_time, "%Y-%m-%dT%H:%M:%S%z").date()
    latitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Latitude', NS).text
    longitude = vehicle_activity.find('.//siri:VehicleLocation/siri:Longitude', NS).text
    line_name_elem = vehicle_activity.find('.//siri:PublishedLineName', NS)
    line_name = line_name_elem.text if line_name_elem is not None else None
    operator_ref = vehicle_activity.find('.//siri:OperatorRef', NS).text
    vehicle_ref = vehicle_activity.find('.//siri:VehicleRef', NS).text
    direction_ref_elem = vehicle_activity.find('.//siri:DirectionRef', NS)
    direction_ref = direction_ref_elem.text if direction_ref_elem is not None else None

    journey_ref_1_elem = vehicle_activity.find('.//siri:FramedVehicleJourneyRef/siri:DatedVehicleJourneyRef', NS)
    journey_ref_2_elem = vehicle_activity.find('.//siri:VehicleJourneyRef', NS)
    journey_ref_1 = journey_ref_1_elem.text if journey_ref_1_elem is not None else None
    journey_ref_2 = journey_ref_2_elem.text if journey_ref_2_elem is not None else None
    journey_ref = journey_ref_1 or journey_ref_2

    if not journey_ref:
        journey_ref = f"{operator_ref}_{line_name}_{vehicle_ref}_{direction_ref}_{date_of_journey}"

    # Assuming service_delivery_timestamp is passed correctly formatted
    response_timestamp = service_delivery_timestamp

    return recorded_at_time, response_timestamp, latitude, longitude, line_name, operator_ref, vehicle_ref, journey_ref, direction_ref, date_of_journey


def validate_data(data):
    valid_data = []
    for record in data:
        try:
            # Update indices according to the new data structure
            # Assuming latitude and longitude are now at indices 2 and 3 respectively
            lat, long = float(record[2]), float(record[3])
            valid_data.append(record)
        except ValueError as e:
            logging.error(f"Data validation error: {e}")
    return valid_data

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.executescript('''
    CREATE TABLE IF NOT EXISTS Operators
        (OperatorID INTEGER PRIMARY KEY AUTOINCREMENT, OperatorRef TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS LineNames
        (LineID INTEGER PRIMARY KEY AUTOINCREMENT, LineName TEXT, OperatorID INTEGER,
         UNIQUE(LineName, OperatorID),
         FOREIGN KEY(OperatorID) REFERENCES Operators(OperatorID));
    CREATE TABLE IF NOT EXISTS Journeys
        (JourneyID INTEGER PRIMARY KEY AUTOINCREMENT, LineID INTEGER, JourneyRef TEXT,
         DateOfJourney DATE, DirectionRef TEXT,
         UNIQUE(LineID, JourneyRef, DateOfJourney, DirectionRef),
         FOREIGN KEY(LineID) REFERENCES LineNames(LineID));
    CREATE TABLE IF NOT EXISTS Positions
        (PositionID INTEGER PRIMARY KEY AUTOINCREMENT, JourneyID INTEGER, RecordedAtTime TEXT, ResponseTimestamp TEXT,
         Latitude TEXT, Longitude TEXT, VehicleRef TEXT,
         UNIQUE(VehicleRef, RecordedAtTime),
         FOREIGN KEY(JourneyID) REFERENCES Journeys(JourneyID));
    ''')
    conn.commit()
    return conn

def insert_data(conn, data):
    c = conn.cursor()

    # Batch list for positions
    positions_to_insert = []

    for record in data:
        recorded_at_time, response_timestamp, latitude, longitude, line_name, operator_ref, vehicle_ref, journey_ref, direction_ref, date_of_journey = record
        line_name = line_name if line_name else 'None'

        # Insert or ignore for Operators
        c.execute("INSERT OR IGNORE INTO Operators (OperatorRef) VALUES (?)", (operator_ref,))
        operator_id = c.execute("SELECT OperatorID FROM Operators WHERE OperatorRef = ?", (operator_ref,)).fetchone()[0]

        # Insert or ignore for LineNames
        c.execute("INSERT OR IGNORE INTO LineNames (LineName, OperatorID) VALUES (?, ?)", (line_name, operator_id))
        line_id = c.execute("SELECT LineID FROM LineNames WHERE LineName = ? AND OperatorID = ?", (line_name, operator_id)).fetchone()[0]

        # Insert or ignore for Journeys
        c.execute(
            """INSERT OR IGNORE INTO Journeys (LineID, JourneyRef, DateOfJourney, DirectionRef) VALUES (?, ?, ?, ?)""",
            (line_id, journey_ref, date_of_journey, direction_ref))
        journey_id = c.execute(
            """SELECT JourneyID FROM Journeys WHERE LineID = ? AND JourneyRef = ? AND DateOfJourney = ? AND DirectionRef = ?""",
            (line_id, journey_ref, date_of_journey, direction_ref)).fetchone()
        if journey_id is not None:
            journey_id = journey_id[0]
        else:
            # Handle the case where journey_id is None
           logging.debug("Journey not found", line_name, line_id, operator_id, recorded_at_time)

        # Prepare position data for batch insert
        positions_to_insert.append((journey_id, recorded_at_time, response_timestamp, latitude, longitude, vehicle_ref))

    # Batch insert for Positions
    if positions_to_insert:
        c.executemany(
            "INSERT INTO Positions (JourneyID, RecordedAtTime, ResponseTimestamp, Latitude, Longitude, VehicleRef) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (RecordedAtTime, VehicleRef) DO NOTHING",
            positions_to_insert)

    conn.commit()


