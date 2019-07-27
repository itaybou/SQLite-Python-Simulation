# Submitters:
# Itay Bouganim, 305278384
# Sahar Vaya, 205583453

# !file: create_db.py
import sys
import os.path
import sqlite3
from sqlite3 import Error

__db_name = "schedule.db"


def main():
    if len(sys.argv) != 2:
        print("Expected execute command format is: python3 create_db.py <config_path>")
        return
    # If db file already exist, remove file in order to start a new db with given config
    elif os.path.isfile(__db_name):
        os.remove(__db_name)
    db_creator = DBCreator(__db_name)
    db_creator.open_db()
    # If all given data from argument given config path was successfully inserted to db, initiated will be true
    initiated = db_creator.init_db(sys.argv[1])
    if initiated:
        # Print current db tables status
        db_creator.print_db_state()
    db_creator.close_db()


class DBCreator:
    __conn = None
    __db_name = ""

    def __init__(self, db_name):
        self.__db_name = db_name

    def init_db(self, config_path):
        if self.__conn is not None:
            # Create db tables, courses, classrooms and students
            self.__create_tables()
            # Parse input to insert to db from config file
            data = self.__parse_config(config_path)
            if data:
                # If data parsed successfully from config file, inserted parsed data to db
                self.__store_data_db(data)
                # Return true since db successfully initiated and config data stored
                return True

    def open_db(self):
        try:
            conn = sqlite3.connect(self.__db_name)
            self.__conn = conn
            conn.text_factory = str
        except Error as e:
            print(e)

    def __create_tables(self):
        # Initiates create tables SQL script
        self.__conn.executescript("""
            CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY,
            course_name TEXT NOT NULL,
            student TEXT NOT NULL,
            number_of_students INTEGER NOT NULL,
            class_id INTEGER REFERENCES classrooms(id),
            course_length INTEGER NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS classrooms (
            id INTEGER PRIMARY KEY,
            location TEXT NOT NULL,
            current_course_id INTEGER NOT NULL,
            current_course_time_left INTEGER NOT NULL
            );
    
            CREATE TABLE IF NOT EXISTS students (
            grade TEXT PRIMARY KEY,
            count INTEGER NOT NULL
            );
        """)

    def __parse_config(self, config_path):
        data = []
        try:
            # Open config file from path
            with open(config_path) as config:
                for line in config:
                    # For each line in the config add a list of all parameters to the data list
                    params = line.replace('\n', '').split(',')
                    data.append(params)
                return data
        except OSError as e:
            print("Config file was not found! " + e.strerror)

    def __store_data_db(self, data):
        for params in data:
            table_id = params[0]
            # Store by data type identifier the data from each list of parameters to the correct SQL table
            if table_id == 'C':
                self.__conn.execute("""
                INSERT INTO courses (id, course_name, student, number_of_students, class_id, course_length) VALUES (?, ?, ?, ?, ?, ?)""",
                [int(params[1]), params[2].strip(), params[3].strip(), int(params[4]), int(params[5]), int(params[6])]),
            elif table_id == 'S':
                self.__conn.execute("""INSERT INTO students (grade, count) VALUES (?, ?)""",
                [params[1].strip(), int(params[2])]),
            elif table_id == 'R':
                self.__conn.execute("""
                INSERT INTO classrooms (id, location, current_course_id, current_course_time_left) VALUES (?, ?, ?, ?)""",
                [int(params[1]), params[2].strip(), 0, 0]),

    def print_db_state(self):
        # Iterate through db table name and print each table
        table_names = self.__conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for name in table_names:
            self.__print_table_state(name[0])

    def __print_table_state(self, table_name):
        print(table_name)
        stmt = "SELECT * FROM {}".format(table_name)
        query_data = self.__conn.cursor()
        query_data.execute(stmt)
        # Print each row in given table name
        for tuple in query_data.fetchall():
            print(tuple)

    def close_db(self):
        self.__conn.commit()
        self.__conn.close()


if __name__ == '__main__':
    main()
