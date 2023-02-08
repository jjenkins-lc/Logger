#native packages
import csv, os, socket, argparse
from datetime import datetime
from scripts.utils import timeit

#3rd party packages
import mysql.connector
import pandas as pd

#use human schema for testing

#linux = posix, win = nt, will need to replace with actual paths.
if os.name == 'nt':
    csv_file = 'C:/tmp/path/to/file'
if os.name == 'posix':
    csv_file = '/tmp/path/to/file'


class Connect:

    """Connection class so we dont lose the weakref when using with open() context manager"""

    #move hardcoded vals to env vars or keyvault
    def __init__(self):
        self.conn = mysql.connector.connect(host = '192.168.23.200', user='root', password='FJ8174620749', database='human', allow_local_infile=True) 
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()

@timeit
def create_temp_table(schema, table_name, csv_file):

    if args.headers:
        headers = [*args.headers]
    else:
        headers = []
        try: 
            with open(csv_file) as f:
                csv_reader = csv.reader(f, delimiter = ',')
                for row in csv_reader:
                    print(row)
                    headers = row
                    #break after first iteration sicne we only want column headers
                    break
        except:
            raise Exception("Something went wrong with extracting column headers from the csv file")

    try:
        #TODO? support more than varchars
        if "id" in headers:
            headers = [x for x in headers if x != "id"]
            header_insert = " VARCHAR(50) NOT NULL, ".join(headers) + " VARCHAR(50) NOT NULL"
            id_column = "id int NOT NULL AUTO_INCREMENT, "
            cmd = "CREATE TABLE IF NOT EXISTS {}.{} ({}{}, PRIMARY KEY (id))".format(schema, table_name, id_column, header_insert)
            conn.cursor.execute(cmd)
            return table_name
        else:
            header_insert = " VARCHAR(50) NOT NULL, ".join(headers[0]) + " VARCHAR(50) NOT NULL"
            id_column = "id int NOT NULL AUTO_INCREMENT, "
            cmd = "CREATE TABLE IF NOT EXISTS {}.{} ({}{}, PRIMARY KEY (id))".format(schema, table_name, id_column, header_insert)
            conn.cursor.execute(cmd)
            return table_name
    
    except mysql.connector.Error as err:
        print("something went wrong with creating the table {}.{}\nError:{}".format(schema, table_name, err))

@timeit
def remove_temp_table(schema, table_name):
    try:
        cmd = "DROP TABLE IF EXISTS {}.{}".format(schema, table_name)
        conn.cursor.execute(cmd)
    except mysql.connector.Error as err:
        print("something went wrong with Dropping the table {}.{}\nError:{}".format(schema, table_name, err))

@timeit
def import_csv_to_temp_table(schema, csv_file, table_name):
    temp_csv = False

    if args.tail:
        temp_csv = True
        df = pd.read_csv(csv_file, engine="pyarrow")
        tail = df.tail(args.tail)
        #TODO need support to look in multiple files, how do we know which files? nameing convention?
        #if len(tail) < args.tail:
        #    df = pd.read_csv(csv_file, engine="pyarrow")

        tail.to_csv('temp_csv.csv', index=False)
        csv_file = 'temp_csv.csv'
        
    try:
        cmd = """
        LOAD DATA LOCAL INFILE '{}' 
        INTO TABLE `{}`.`{}` 
        FIELDS TERMINATED BY ',' 
        LINES TERMINATED BY '\n'
        """.format(csv_file, schema, table_name)
        conn.cursor.execute(cmd)
        conn.conn.commit()

    except mysql.connector.errors.DataError as err:
        print("something is wrong with the file, please check field names and types to match for table {}\nError: {}".format(table_name, err))

    except mysql.connector.Error as err:
        print("something went wrong with importing to table {}.{}\nError:{}".format(schema, table_name, err))
    
    finally:
        if temp_csv:
            os.remove('temp_csv.csv')


def main():
    table_name = socket.gethostname().replace("-", "")

    parser = argparse.ArgumentParser(
        prog='csv to MySQL DB',
        description='Take a specified file and import it to MySQL database'
    )
    parser.add_argument('-f', '--file', nargs='?', type=str, default='bigtest.csv', help="Specify desired file to import into the Database.\n")
    parser.add_argument('-t','--tail', nargs='?', type=int, default=100000, help="Import the last N of rows of the csv. If unspecified the default is the last 100,000 rows\n")
    parser.add_argument('--table', nargs='?', type=str, default=table_name, help="Specify the MySQL table to import data to, If unspecified the default is the hostname of the machine this is ran on. Table name will need to follow MySQL naming conventions and restrictions.\n")
    parser.add_argument('--schema', nargs='?', type=str, default="human", help="Specify the MySQL schema to use to import data to, if unspecified the default is 'human'.\n")
    parser.add_argument('-ch','--headers', nargs='+', type=str, default=None, help="Specify the colunns to create for a temporary table, by default it will use column headers specified in the first row of the csv.")
    args = parser.parse_args()
    conn = Connect()
    print("connected to db")
    create_temp_table(schema=args.schema, table_name=args.table, csv_file=args.file)
    print("created table, {}".format(table_name))
    print("uploading csv to db")
    import_csv_to_temp_table(schema=args.schema, csv_file=args.file, table_name=args.table)
    remove_temp_table(schema="human", table_name = table_name)
    conn.close()

if __name__ == "__main__":
    main()
    