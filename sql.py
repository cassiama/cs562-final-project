import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    
    input_file_no = int(input("Input number for input file: "))
    input_notes = 'Please make your number between 1 and 6.'
    while input_file_no <= 0 or input_file_no > 6:
        print(input_notes)
        input_file_no = int(input("Input number for input file: "))
    filepath = f'./q{input_file_no}_sql.txt'
    with open(filepath, "r") as f:
        phi = f.read()

    cur.execute(phi)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
