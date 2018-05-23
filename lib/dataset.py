import sqlite3
from os import path, listdir

class Dataset(object):
    def __init__(self, data_directory):
        self.data_directory = data_directory 
        if not self.data_directory.endswith("/"):
            self.data_directory += "/"
        self.connections = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        print("closing and committing datasets")
        self.commit_all()
        for conn in self.connections.values():
            conn.close()
    
    def get_databases(self):
        for fname in listdir(self.data_directory):
            if not fname.endswith(".sqlite3"): continue 
            yield tuple(fname[:-8].split("_"))

    def location_for_az_insttype(self, az, insttype):
        return "%s%s_%s.sqlite3" % (self.data_directory, az, insttype)
    
    def open(self, az, insttype):
        key = (az, insttype)
        if key in self.connections: 
            return self.connections[key]
        else:
            loc = self.location_for_az_insttype(az, insttype)
            if not path.exists(loc):
                raise Exception("no data for az, insttype pair %s, %s" % (az, insttype))
            conn = sqlite3.connect(loc)
            self.connections[key] = conn 
            return conn 
    
    def open_or_create(self, az, insttype):
        loc = self.location_for_az_insttype(az, insttype)
        if path.exists(loc):
            return self.open(az, insttype)
        else:
            conn = sqlite3.connect(loc)
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS prices (timestamp INTEGER PRIMARY KEY NOT NULL, price REAL NOT NULL)")
            conn.commit()
            conn.close()
            return self.open(az, insttype)

    """
        used in conjunction with commit_all to insert data when importing a dataset
    """
    def insert_data(self, az, insttype, timestamp, price):
        conn = self.open_or_create(az, insttype)
        c = conn.cursor()
        c.execute("REPLACE INTO prices VALUES (%d, %f)" % (timestamp, price))

    def commit_all(self):
        for conn in self.connections.values():
            conn.commit()
    """
        helpful accessor functions
    """
    def run_on_all_tables(self, command):
        for az, insttype in self.get_databases():
            conn = self.open(az, insttype)
            c = conn.cursor()
            c.execute(command)
            conn.commit()

    """
        get dataset
    """
    def get_dataset(self, az, isnttype):
        conn = self.open_or_create(az, insttype)
        c = conn.cursor()
        return PriceData(c.fetchall())