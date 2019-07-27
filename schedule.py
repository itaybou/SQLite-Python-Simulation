# Submitters:
# Itay Bouganim, 305278384
# Sahar Vaya, 205583453

# !file: schedule.py
import os.path
import sqlite3
from sqlite3 import Error

__db_name = "schedule.db"


def main():
    if os.path.isfile(__db_name):
        conn = _open_db()
    else:
        print("Database {} does not exist!".format(__db_name))
        return
    if conn is not None:
        scheduler = Scheduler(conn, __db_name)
        scheduler.schedule()
        scheduler.close_db()


def _open_db():
    try:
        conn = sqlite3.connect(__db_name)
        conn.text_factory = str
        return conn
    except Error as e:
        print(e)
    return None


class Scheduler:
    __conn = None
    __db_name = ""

    def __init__(self, conn, db_name):
        self.__conn = conn
        self.__db_name = db_name

    def schedule(self):
        iteration = 0
        # For each iteration until all courses are completed, or DB has been removed
        while os.path.isfile(self.__db_name) and not self.__is_courses_done():
            stmt = "SELECT * FROM (SELECT * FROM courses ORDER BY ROWID DESC) JOIN classrooms class " \
                   "ON class_id = class.id GROUP BY class_id LIMIT(SELECT COUNT(DISTINCT id) FROM classrooms)"
            query_data = self.__conn.cursor()
            query_data.execute(stmt)
            # Get all courses and classrooms information ordered by insertion
            for tuple in query_data.fetchall():
                self.__update_and_print_course_status(iteration, tuple)  # Update and print course status in class
            self.__print_db_state()
            iteration += 1
        if iteration == 0:  # No iteration has been made, all courses are done
            self.__print_db_state()

    def __is_courses_done(self):
        stmt = "SELECT COUNT(id) FROM courses"
        query_data = self.__conn.cursor()
        query_data.execute(stmt)
        # Checks if courses table is empty, indicating all courses are done
        if query_data.fetchone()[0] == 0:
            return True
        return False

    def __update_and_print_course_status(self, iteration, tuple):
        course_time_left = tuple[9]
        class_location = tuple[7]
        course_name = tuple[1]
        if course_time_left == 0:
            self.__update_course_time_in_class(True, tuple[0], tuple[4], tuple[5])  # Update a new course in class
            print("({}) {}: {} is schedule to start".format(iteration, class_location, course_name))
        else:
            # Update the course time left in class
            is_done = self.__update_course_time_in_class(False, tuple[0], tuple[4], tuple[5])
            if is_done:  # if the course is done in current iteration, assign a new course if available
                print("({}) {}: {} is done".format(iteration, class_location, course_name))
                started = self.__check_start_new_course(iteration)
                if started is not None:
                    print(started)
            else:  # if the course is not done indicate that class is still occupied
                print("({}) {}: occupied by {}".format(iteration, class_location, course_name))

    def __update_course_time_in_class(self, init, course_id, class_id, course_duration):
        stmt = "SELECT id, current_course_time_left, current_course_id FROM classrooms class WHERE class.id = {}".format(class_id)
        query_data = self.__conn.cursor()
        query_data.execute(stmt)
        # Get current classroom info according to class id
        tuple = query_data.fetchone()
        if tuple is not None:
            if init is False:  # Meaning classroom is already assigned to a course that is still active
                time_left = tuple[1]
                is_done = time_left - 1 == 0  # Indicates if this is the final iteration for current course
                current_course = 0 if is_done else tuple[2]
                # Update course remaining time in class with the current time left - 1
                self.__update_course_time_left(time_left-1, current_course, class_id)
                if is_done:  # Course is done, will be removed from courses table
                    self.__remove_course(course_id)
                    return True
            else:  # A course is assigned to a new class
                self.__update_course_time_left(course_duration, course_id, class_id)
                # Remove the student amount started a course from student table
                self.__remove_available_students(course_id)
            self.__conn.commit()

    def __check_start_new_course(self, iteration):
        stmt = "SELECT * FROM (SELECT * FROM courses ORDER BY ROWID DESC) JOIN classrooms class " \
               "ON class_id = class.id WHERE current_course_time_left = 0 GROUP BY class_id LIMIT(1)"
        query_data = self.__conn.cursor()
        query_data.execute(stmt)
        # Get one course that has not been started yet, and that is to be assigned to relevant class
        tuple = query_data.fetchone()
        if tuple is not None:
            # Update course status to start in current iteration
            self.__update_course_time_in_class(True, tuple[0], tuple[4], tuple[5])
            return "({}) {}: {} is schedule to start".format(iteration, tuple[7], tuple[1])

    def __update_course_time_left(self, current_time_left, course_id, class_id):
        self.__conn.execute("""UPDATE classrooms SET current_course_time_left = ?, current_course_id = ? WHERE id = ?""",
        [int(current_time_left), int(course_id), int(class_id)])

    def __remove_available_students(self, course_id):
        self.__conn.execute("""UPDATE students SET count = count - (SELECT number_of_students FROM courses WHERE id = ?)
                                WHERE grade = (SELECT student FROM courses WHERE id = ?)""",
         [int(course_id), int(course_id)])
        self.__conn.commit()

    def __remove_course(self, course_id):
        self.__conn.execute("""DELETE FROM courses WHERE id = ?""",
        [int(course_id)])
        self.__conn.commit()

    def __print_db_state(self):
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
