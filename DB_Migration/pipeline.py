# pipeline.py
"""
ETL Pipeline for Migrating Data from MySQL to MongoDB

This script implements a robust, modular, and maintainable ETL pipeline to extract data from a MySQL database,
transform it according to mapping rules (including type conversion and relationship handling),
and load the data into a MongoDB database. The design adheres to SOLID principles:
 - Single Responsibility: Each class (MySQLExtractor, DataTransformer, MongoLoader) handles one concern.
 - Open/Closed: The system can be extended via subclassing or new modules without modifying existing code.
 - Dependency Injection: Configuration settings are injected.

Error handling, logging, and comments are provided to support both positive and negative scenarios.

Testing suggestions and monitoring recommendations are included at the bottom of the file.
"""

import pymysql
import pymongo
import logging
import datetime
from pymongo.errors import BulkWriteError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


class MySQLExtractor:
    """
    Handles database connection and data extraction from MySQL.

    Attributes:
        host (str): MySQL server hostname.
        user (str): Username for MySQL.
        password (str): Password for MySQL.
        database (str): Database name.
        port (int): Port number.
    """
    def __init__(self, host, user, password, database, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """Establishes a connection to the MySQL database."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                cursorclass=pymysql.cursors.DictCursor
            )
            logging.info('MySQL connection established')
        except Exception as exc:
            logging.error('Error connecting to MySQL: %s', exc)
            raise

    def extract(self, query):
        """Executes a given SQL query and returns the results as a list of dictionaries."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return results
        except Exception as exc:
            logging.error('Error executing query "%s": %s', query, exc)
            raise

    def close(self):
        """Closes the MySQL database connection."""
        if self.connection:
            self.connection.close()
            logging.info('MySQL connection closed')


class MongoLoader:
    """
    Handles loading documents into MongoDB.

    Attributes:
        uri (str): MongoDB connection URI.
        db_name (str): MongoDB database name.
    """
    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name
        try:
            self.client = pymongo.MongoClient(self.uri)
            self.db = self.client[self.db_name]
            logging.info('Connected to MongoDB')
        except Exception as exc:
            logging.error('Error connecting to MongoDB: %s', exc)
            raise

    def load_collection(self, collection_name, documents, batch_size=1000):
        """
        Loads documents into a MongoDB collection.

        Deletes existing content in the collection before loading new data.
        Performs batch inserts with error handling.
        """
        try:
            collection = self.db[collection_name]
            # Optional: Clear the collection if a full refresh is intended
            collection.delete_many({})
            if documents:
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i+batch_size]
                    collection.insert_many(batch, ordered=False)
                logging.info('Loaded %d documents into collection "%s"', len(documents), collection_name)
        except BulkWriteError as bwe:
            logging.error('Bulk write error: %s', bwe.details)
            raise
        except Exception as exc:
            logging.error('Error loading documents into collection "%s": %s', collection_name, exc)
            raise

    def get_collection(self, collection_name):
        return self.db[collection_name]


class DataTransformer:
    """
    Transforms raw MySQL data to MongoDB document format, handling type conversions,
    field mappings, and embedding of related data as defined in the mapping document.
    """
    def transform_employee(self, employee, dept_emps, dept_managers, salaries, titles, departments_lookup):
        """
        Transforms a single employee record by embedding department, salary, title, and managerial data.

        Args:
            employee (dict): Record from the employees table.
            dept_emps (list): Associated department records from dept_emp table.
            dept_managers (list): Managerial records from dept_manager table.
            salaries (list): Salary records from salaries table.
            titles (list): Title records from titles table.
            departments_lookup (dict): Dictionary mapping dept_no to department details.

        Returns:
            dict: A transformed employee document ready for MongoDB.
        """
        try:
            emp_doc = {
                'emp_no': employee['emp_no'],
                'birth_date': self.convert_date(employee['birth_date']),
                'first_name': employee['first_name'],
                'last_name': employee['last_name'],
                'gender': employee['gender'],
                'hire_date': self.convert_date(employee['hire_date']),
                'departments': [],
                'salaries': [],
                'titles': [],
                'management': None
            }

            # Embed department information
            for de in dept_emps:
                dept_no = de['dept_no']
                department = departments_lookup.get(dept_no, {'dept_no': dept_no, 'dept_name': 'Unknown'})
                emp_doc['departments'].append({
                    'dept_no': department['dept_no'],
                    'dept_name': department['dept_name'],
                    'from_date': self.convert_date(de['from_date']),
                    'to_date': self.convert_date(de['to_date'])
                })

            # Embed salary records
            for sal in salaries:
                emp_doc['salaries'].append({
                    'salary': sal['salary'],
                    'from_date': self.convert_date(sal['from_date']),
                    'to_date': self.convert_date(sal['to_date'])
                })

            # Embed title records
            for tit in titles:
                emp_doc['titles'].append({
                    'title': tit['title'],
                    'from_date': self.convert_date(tit['from_date']),
                    'to_date': self.convert_date(tit['to_date']) if tit['to_date'] else None
                })

            # Embed managerial record (if exists, pick the first record)
            if dept_managers:
                mgr = dept_managers[0]
                emp_doc['management'] = {
                    'dept_no': mgr['dept_no'],
                    'from_date': self.convert_date(mgr['from_date']),
                    'to_date': self.convert_date(mgr['to_date'])
                }

            return emp_doc
        except Exception as exc:
            logging.error('Error transforming employee %s: %s', employee.get('emp_no', '<unknown>'), exc)
            raise

    def convert_date(self, date_str):
        """
        Converts a MySQL date (YYYY-MM-DD string) to a Python datetime object.
        If the value is already a date object, it is returned unchanged.
        """
        try:
            if isinstance(date_str, (datetime.date, datetime.datetime)):
                return date_str
            return datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as exc:
            logging.error('Date conversion error for value %s: %s', date_str, exc)
            raise


def main():
    # Configuration - in production these should be externalized (e.g., environment variables, config files)
    mysql_config = {
        'host': 'localhost',
        'user': 'mysql_user',
        'password': 'mysql_password',
        'database': 'employees_db',
        'port': 3306
    }
    mongo_config = {
        'uri': 'mongodb://localhost:27017',
        'db_name': 'company_db'
    }

    # SQL queries to extract data from MySQL
    queries = {
        'employees': 'SELECT * FROM employees',
        'departments': 'SELECT * FROM departments',
        'dept_emp': 'SELECT * FROM dept_emp',
        'dept_manager': 'SELECT * FROM dept_manager',
        'salaries': 'SELECT * FROM salaries',
        'titles': 'SELECT * FROM titles'
    }

    try:
        # Extract data from MySQL
        extractor = MySQLExtractor(**mysql_config)
        extractor.connect()
        employees = extractor.extract(queries['employees'])
        departments = extractor.extract(queries['departments'])
        dept_emp = extractor.extract(queries['dept_emp'])
        dept_manager = extractor.extract(queries['dept_manager'])
        salaries = extractor.extract(queries['salaries'])
        titles = extractor.extract(queries['titles'])

        # Create a lookup map for department information (by dept_no)
        departments_lookup = {dept['dept_no']: dept for dept in departments}

        # Organize related records by employee id
        dept_emp_by_emp = {}
        for record in dept_emp:
            emp_no = record['emp_no']
            dept_emp_by_emp.setdefault(emp_no, []).append(record)

        dept_manager_by_emp = {}
        for record in dept_manager:
            emp_no = record['emp_no']
            dept_manager_by_emp.setdefault(emp_no, []).append(record)

        salaries_by_emp = {}
        for record in salaries:
            emp_no = record['emp_no']
            salaries_by_emp.setdefault(emp_no, []).append(record)

        titles_by_emp = {}
        for record in titles:
            emp_no = record['emp_no']
            titles_by_emp.setdefault(emp_no, []).append(record)

        transformer = DataTransformer()
        employee_documents = []
        for emp in employees:
            emp_no = emp['emp_no']
            emp_dept_emps = dept_emp_by_emp.get(emp_no, [])
            emp_dept_managers = dept_manager_by_emp.get(emp_no, [])
            emp_salaries = salaries_by_emp.get(emp_no, [])
            emp_titles = titles_by_emp.get(emp_no, [])
            try:
                transformed_doc = transformer.transform_employee(
                    emp, emp_dept_emps, emp_dept_managers, emp_salaries, emp_titles, departments_lookup
                )
                employee_documents.append(transformed_doc)
            except Exception as exc:
                logging.error('Skipping employee %s due to transformation error.', emp_no)

        # Load transformed data into MongoDB
        loader = MongoLoader(uri=mongo_config['uri'], db_name=mongo_config['db_name'])
        loader.load_collection('employees', employee_documents)

        # Additionally, load the departments as a separate collection
        loader.load_collection('departments', departments)

        logging.info('ETL pipeline executed successfully')
    except Exception as e:
        logging.error('ETL pipeline failed: %s', e)
    finally:
        extractor.close()


if __name__ == '__main__':
    main()


# Testing and Monitoring Recommendations:
# -----------------------------------------
# Unit Testing:
#   Use Python’s unittest or pytest framework to write tests for each class:
#   - Test MySQLExtractor.connect() and extract() with a mock MySQL server.
#   - Test DataTransformer.convert_date() with valid and invalid dates.
#   - Test DataTransformer.transform_employee() with controlled input data.
#   - Test MongoLoader.load_collection() using a test MongoDB instance or mocks.
#
# Integration Testing:
#   Run the ETL pipeline in a staging environment with sample data and verify results in MongoDB.
#
# Monitoring & Alerting:
#   - Implement logging aggregation (e.g., ELK stack, Splunk) to monitor errors.
#   - Setup performance and error alerts for the pipeline process using tools like Prometheus or CloudWatch.
#
# Migration Strategies:
#   - For incremental migration, store and use the last updated timestamp as a checkpoint.
#   - Implement rollback strategies by archiving previous data before performing migration operations.
#   - Validate data integrity post-migration by comparing counts and key values between MySQL and MongoDB.

# Note: In production, sensitive information such as database credentials should be managed securely using environment variables
# or secure vault services.
