import logging
from cassandra.cluster import Cluster, BatchStatement


class CassandraConnection:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def create_session(self):
        self.cluster = Cluster(['localhost'])  # 127.0.0.1
        self.session = self.cluster.connect(self.keyspace)

    def get_session(self):
        return self.session

    # How about Adding some log info to see what went wrong
    def set_logger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def create_keyspace(self, keyspace, replication_factor):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            self.log.info("dropping existing keyspace...")
        else:
            self.log.info("creating keyspace...")
            self.session.execute("""
                    CREATE KEYSPACE %s
                    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': %s }
                    """ % (keyspace, replication_factor))
        self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)  # Used to USE the key_space

    def create_table(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS employee (emp_id int PRIMARY KEY,
                                              ename varchar,
                                              sal double,
                                              city varchar);
                 """
        self.session.execute(c_sql)
        self.log.info("Table Created !!!")

    def make_query(self, query):
        self.session.execute(query)
        self.log.info("Query secceed !")

    # lets do some batch insert
    def insert_data(self):
        insert_sql = self.session.prepare("INSERT INTO  employee (emp_id, ename , sal, city) VALUES (?,?,?,?)")
        batch = BatchStatement()
        batch.add(insert_sql, (1, 'LyubovK', 2555, 'Dubai'))
        batch.add(insert_sql, (2, 'JiriK', 5660, 'Toronto'))
        batch.add(insert_sql, (3, 'IvanH', 2547, 'Mumbai'))
        batch.add(insert_sql, (4, 'YuliaT', 2547, 'Seattle'))
        self.session.execute(batch)
        self.log.info('Batch Insert Completed')

    def select_data(self):
        rows = self.session.execute('select * from employee;')
        for row in rows:
            print("Name: {}; Salary: {}; City: {}".format(row.ename, row.sal, row.city))

    def update_data(self):
        pass

    def delete_data(self):
        pass

