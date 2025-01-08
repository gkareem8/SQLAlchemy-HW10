from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
import pandas as pd

# Define the SQLHelper Class
# PURPOSE: Deal with all of the database logic

class SQLHelper:

    # Initialize PARAMETERS/VARIABLES
    #################################################
    # Database Setup
    #################################################
    def __init__(self):
        # Create engine for SQLite database
        self.engine = create_engine("sqlite:///hawaii.sqlite")
        # Reflect the tables only once
        self.Base = automap_base()
        self.Base.prepare(autoload_with=self.engine)
        
        # Bind references to the tables
        self.Station = self.Base.classes.station  # Correct table name from DB
        self.Measurement = self.Base.classes.measurement  # Correct table name from DB

    #################################################
    # ORM Methods
    #################################################
    
    def query_precipitation_orm(self):
        """Return precipitation data using ORM"""
        # Create a session (link) from Python to the DB
        session = Session(self.engine)

        # Query all precipitation data
        rows = session.query(
            self.Measurement.id,
            self.Measurement.station,
            self.Measurement.date,
            self.Measurement.prcp
        ).all()

        # Create DataFrame
        df = pd.DataFrame(rows, columns=['id', 'station', 'date', 'prcp'])

        # Close session
        session.close()

        return df

    def query_precipitation_sql(self):
        """Return precipitation data using raw SQL"""
        # Create connection
        conn = self.engine.connect()

        # Define query
        query = text("""
            SELECT id, station, date, prcp
            FROM measurement
            WHERE date >= '2016-08-23'
            ORDER BY date;
        """)

        # Execute query and load into DataFrame
        df = pd.read_sql(query, con=conn)

        # Close connection
        conn.close()

        return df
