import logging
import sys
import pymysql
import pymongo
from pymongo.errors import BulkWriteError
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# MySQL Extractor handles all data extraction from MySQL
class MySQLExtractor:
    def __init__(self, host, user, password, db, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                port=self.port,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info('Connected to MySQL database.')
        except Exception as e:
            logger.error(f'Error connecting to MySQL: {e}')
            raise

    def fetch_data(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
            logger.info(f'Executed query: {query}')
            return data
        except Exception as e:
            logger.error(f'Error executing query: {query} : {e}')
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('Closed MySQL connection.')


# DataTransformer handles transformation logic, implementing SOLID by only focusing on transformation
class DataTransformer:
    @staticmethod
    def transform_date(date_str):
        # Assuming date_str is in a recognizable format (YYYY-MM-DD)
        try:
            # Convert to Python datetime (MongoDB will store it as ISODate equivalent)
            return datetime.strptime(date_str, '%Y-%m-%d') if date_str else None
        except Exception as e:
            logger.error(f'Error converting date {date_str}: {e}')
            return None

    @staticmethod
    def transform_employee(employee_row):
        # Transform the core employee fields
        transformed = {
            'emp_no': employee_row.get('emp_no'),
            'birth_date': DataTransformer.transform_date(employee_row.get('birth_date')),
            'first_name': employee_row.get('first_name'),
            'last_name': employee_row.get('last_name'),
            'gender': employee_row.get('gender'),
            'hire_date': DataTransformer.transform_date(employee_row.get('hire_date')),
            'titles': [],
            'salaries': [],
            'departments': []
        }
        return transformed

    @staticmethod
    def transform_title(title_row):
        # Maps title record fields: title, from_date, to_date
        return {
            'title': title_row.get('title'),
            'from_date': DataTransformer.transform_date(title_row.get('from_date')),
            'to_date': DataTransformer.transform_date(title_row.get('to_date'))
        }

    @staticmethod
    def transform_salary(salary_row):
        return {
            'salary': salary_row.get('salary'),
            'from_date': DataTransformer.transform_date(salary_row.get('from_date')),
            'to_date': DataTransformer.transform_date(salary_row.get('to_date'))
        }

    @staticmethod
    def transform_department_assignment(dept_emp_row):
        return {
            'dept_no': dept_emp_row.get('dept_no'),
            'from_date': DataTransformer.transform_date(dept_emp_row.get('from_date')),
            'to_date': DataTransformer.transform_date(dept_emp_row.get('to_date'))
        }

    @staticmethod
    def transform_department(dept_row):
        return {
            'dept_no': dept_row.get('dept_no'),
            'dept_name': dept_row.get('dept_name')
        }

    @staticmethod
    def transform_dept_manager(dept_manager_row):
        return {
            'emp_no': dept_manager_row.get('emp_no'),
            'dept_no': dept_manager_row.get('dept_no'),
            'from_date': DataTransformer.transform_date(dept_manager_row.get('from_date')),
            'to_date': DataTransformer.transform_date(dept_manager_row.get('to_date'))
        }


# MongoDBLoader handles loading data into MongoDB
class MongoDBLoader:
    def __init__(self, uri, db_name):
        try:
            self.client = pymongo.MongoClient(uri)
            self.db = self.client[db_name]
            logger.info('Connected to MongoDB.')
        except Exception as e:
            logger.error(f'Error connecting to MongoDB: {e}')
            raise

    def insert_employees(self, employees):
        if not employees:
            logger.warning('No employee records to insert.')
            return
        try:
            result = self.db.employees.insert_many(employees, ordered=False)
            logger.info(f'Inserted {len(result.inserted_ids)} employee records.')
        except BulkWriteError as bwe:
            logger.error(f'Bulk write error inserting employees: {bwe.details}')
        except Exception as e:
            logger.error(f'Error inserting employees: {e}')

    def insert_departments(self, departments):
        if not departments:
            logger.warning('No department records to insert.')
            return
        try:
            result = self.db.departments.insert_many(departments, ordered=False)
            logger.info(f'Inserted {len(result.inserted_ids)} department records.')
        except BulkWriteError as bwe:
            logger.error(f'Bulk write error inserting departments: {bwe.details}')
        except Exception as e:
            logger.error(f'Error inserting departments: {e}')

    def insert_dept_managers(self, dept_managers):
        if not dept_managers:
            logger.warning('No dept_manager records to insert.')
            return
        try:
            result = self.db.dept_manager.insert_many(dept_managers, ordered=False)
            logger.info(f'Inserted {len(result.inserted_ids)} dept_manager records.')
        except BulkWriteError as bwe:
            logger.error(f'Bulk write error inserting dept_manager: {bwe.details}')
        except Exception as e:
            logger.error(f'Error inserting dept_manager: {e}')

    def close(self):
        self.client.close()
        logger.info('Closed MongoDB connection.')


# ETLPipeline orchestrates the complete ETL process
class ETLPipeline:
    def __init__(self, mysql_config, mongodb_config):
        self.mysql_extractor = MySQLExtractor(**mysql_config)
        self.mongodb_loader = MongoDBLoader(**mongodb_config)
        self.transformer = DataTransformer()
        self.employees = {}  # key: emp_no, value: transformed employee document
        self.departments = []
        self.dept_managers = []

    def extract(self):
        try:
            self.mysql_extractor.connect()
            # Extract core employee data
            employee_query = "SELECT * FROM employees"
            employees_data = self.mysql_extractor.fetch_data(employee_query)

            # Extract titles data
            title_query = "SELECT * FROM titles"
            titles_data = self.mysql_extractor.fetch_data(title_query)

            # Extract salary data
            salary_query = "SELECT * FROM salaries"
            salaries_data = self.mysql_extractor.fetch_data(salary_query)

            # Extract department assignments (dept_emp)
            dept_emp_query = "SELECT * FROM dept_emp"
            dept_emp_data = self.mysql_extractor.fetch_data(dept_emp_query)

            # Extract departments
            dept_query = "SELECT * FROM departments"
            departments_data = self.mysql_extractor.fetch_data(dept_query)

            # Extract department managers
            dept_manager_query = "SELECT * FROM dept_manager"
            dept_manager_data = self.mysql_extractor.fetch_data(dept_manager_query)

            return {
                'employees': employees_data,
                'titles': titles_data,
                'salaries': salaries_data,
                'dept_emp': dept_emp_data,
                'departments': departments_data,
                'dept_manager': dept_manager_data
            }
        except Exception as e:
            logger.error(f'Error during extraction: {e}')
            raise
        finally:
            self.mysql_extractor.close()

    def transform(self, data):
        try:
            # Transform employees
            for emp in data.get('employees', []):
                transformed_emp = self.transformer.transform_employee(emp)
                self.employees[transformed_emp['emp_no']] = transformed_emp

            # Transform titles and embed into employee documents
            for title in data.get('titles', []):
                emp_no = title.get('emp_no')
                if emp_no in self.employees:
                    transformed_title = self.transformer.transform_title(title)
                    self.employees[emp_no]['titles'].append(transformed_title)
                else:
                    logger.warning(f'Employee emp_no {emp_no} not found for title record.')

            # Transform salaries and embed
            for salary in data.get('salaries', []):
                emp_no = salary.get('emp_no')
                if emp_no in self.employees:
                    transformed_salary = self.transformer.transform_salary(salary)
                    self.employees[emp_no]['salaries'].append(transformed_salary)
                else:
                    logger.warning(f'Employee emp_no {emp_no} not found for salary record.')

            # Transform dept_emp (department assignments) and embed
            for dept in data.get('dept_emp', []):
                emp_no = dept.get('emp_no')
                if emp_no in self.employees:
                    transformed_dept = self.transformer.transform_department_assignment(dept)
                    self.employees[emp_no]['departments'].append(transformed_dept)
                else:
                    logger.warning(f'Employee emp_no {emp_no} not found for department assignment.')

            # Transform departments
            for dept in data.get('departments', []):
                transformed_dept = self.transformer.transform_department(dept)
                self.departments.append(transformed_dept)

            # Transform dept_manager
            for dm in data.get('dept_manager', []):
                transformed_dm = self.transformer.transform_dept_manager(dm)
                self.dept_managers.append(transformed_dm)

            return {
                'employees': list(self.employees.values()),
                'departments': self.departments,
                'dept_manager': self.dept_managers
            }
        except Exception as e:
            logger.error(f'Error during transformation: {e}')
            raise

    def load(self, transformed_data):
        try:
            # Load departments to MongoDB
            self.mongodb_loader.insert_departments(transformed_data.get('departments', []))

            # Load employees with embedded title, salary and dept assignments
            self.mongodb_loader.insert_employees(transformed_data.get('employees', []))

            # Load department managers to MongoDB
            self.mongodb_loader.insert_dept_managers(transformed_data.get('dept_manager', []))
        except Exception as e:
            logger.error(f'Error during loading: {e}')
            raise
        finally:
            self.mongodb_loader.close()

    def run(self):
        logger.info('ETL pipeline started.')
        try:
            extracted_data = self.extract()
            transformed_data = self.transform(extracted_data)
            self.load(transformed_data)
            logger.info('ETL pipeline completed successfully.')
        except Exception as e:
            logger.error(f'ETL pipeline failed: {e}')
            # For rollback, you might implement additional logic here
            sys.exit(1)


if __name__ == '__main__':
    # Configurations for connections (These could be loaded from a config file/environment variables)
    mysql_config = {
        'host': 'localhost',
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'db': 'your_database',
        'port': 3306
    }

    mongodb_config = {
        'uri': 'mongodb://localhost:27017/',
        'db_name': 'your_mongo_database'
    }

    etl_pipeline = ETLPipeline(mysql_config, mongodb_config)
    etl_pipeline.run()

    # Testing & Monitoring Recommendations:
    # 1. Unit test each module (Extractor, Transformer, Loader) using Python's unittest or pytest.
    # 2. Integration tests to ensure end-to-end data integrity and performance.
    # 3. Use monitoring tools (e.g., Prometheus, ELK stack) for production migration monitoring.
    # 4. Build incremental migration support by recording the last processed record and use that in subsequent runs.
    # 5. Implement rollback by either using transactions (if supported) or by maintaining backup snapshots.

    # This script follows SOLID principles: each class has a single responsibility,
    # strict separation between extraction, transformation, and load processes, and clear error handling.
