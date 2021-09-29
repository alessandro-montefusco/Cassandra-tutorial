import barnum
import findspark
import random
import uuid
from cassandra.cluster import BatchStatement
from pyspark import SparkConf, SparkContext, SQLContext, Row
from cassandra_connection import CassandraConnection


def init():
    findspark.init()
    findspark.add_packages(["com.datastax.spark:spark-cassandra-connector_2.12:3.0.1"])  # Add the Cassandra connector
    findspark.add_packages(['org.apache.spark:spark-streaming-kafka-0-10_2.11:3.1.1'])
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'


def populate_keyspace():
    # Create an employee table
    # emp_id | first_name | last_name | email | password | department | city | phone | date_birth | cards |
    # cluster keys: last_name, fisrt_name
    query_employee = """ 
                CREATE TABLE IF NOT EXISTS employee_by_id (
                emp_id uuid, first_name text, last_name text, email text, password text, 
                department text, city text, phone text, birth date, cards text,
                PRIMARY KEY (emp_id, last_name, first_name) 
                );
                """
    # Create an employee table focused on departments
    query_department = """ 
                    CREATE TABLE IF NOT EXISTS employee_by_department (
                    emp_id uuid, department text, city text, email text, cards text,
                    PRIMARY KEY ((emp_id, department), city) 
                    );
                    """
    con.make_query(query_employee)
    con.make_query(query_department)

    # Populate tables randomly
    session = con.get_session()
    insert_employee = session. \
        prepare("INSERT INTO  employee_by_id "
                "(emp_id, first_name, last_name, email, password, department, city, phone, birth, cards) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)")
    insert_department = session. \
        prepare("INSERT INTO  employee_by_department"
                "(emp_id, department, city, email, cards)"
                "VALUES (?,?,?,?,?)")
    batch_1 = BatchStatement()
    batch_2 = BatchStatement()
    departments = ["IT", "HR", "AI", "CO"]
    cities = ["Milano", "Roma", "Napoli", "Torino"]

    for i in range(40):
        id = uuid.uuid1()
        first_name, last_name = barnum.create_name(full_name=True)
        email = barnum.create_email()
        department = random.choice(departments)
        city = random.choice(cities)
        card = barnum.create_cc_number()[1][0]
        batch_1.add(insert_employee, (id, first_name, last_name, email, barnum.create_pw(length=10),
                                      department, city, barnum.create_phone(), barnum.create_date(), card))
        batch_2.add(insert_department, (id, department, city, email, card))
    session.execute(batch_1)
    session.execute(batch_2)


if __name__ == '__main__':
    init()
    con = CassandraConnection()
    con.create_session()
    con.set_logger()
    #con.create_keyspace(keyspace='remote', replication_factor=2)
    #populate_keyspace()

    conf = SparkConf().setAppName("spark_cassandra_test")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Load the DataFrame by linking it to a Cassandra table
    emp_by_id = sqlContext.read.format("org.apache.spark.sql.cassandra") \
        .options(table="employee_by_id", keyspace="testkeyspace") \
        .load().show()  # rdd object
    '''
    emp_by_department = sqlContext.read.format("org.apache.spark.sql.cassandra") \
        .options(table="employee_by_department", keyspace="testkeyspace") \
        .load().rdd  # rdd object

    rdd1 = emp_by_id.filter(lambda row: row['department'] == 'HR')\
        .map(lambda row: (Row(emp_id=row['emp_id'], full_name=row['first_name'].upper() + " " + row['last_name'].upper())))
    df1 = rdd1.toDF(schema=['emp_id', 'full_name'])

    rdd2 = emp_by_department.filter((lambda row: row['department'] == 'HR'))
    df2 = rdd2.toDF(schema=['emp_id', 'department', 'city', 'cards', 'email'])
    df = df1.join(df2, on=['emp_id'])
    #print(df.show())  # emp_id | full_name | cards | city | cards | email|

    # Save operation in Cassandra as new table
    query = """
            CREATE TABLE IF NOT EXISTS employee_hr (
            emp_id uuid, full_name text, department text, city text, cards text, email text,
            PRIMARY KEY (emp_id, full_name) 
            );
            """
    #con.make_query(query1)
    df1.write.format("org.apache.spark.sql.cassandra") \
        .mode('overwrite') \
        .options(table="employee_hr", keyspace="testkeyspace") \
        .save()

    sc.stop()
    '''


