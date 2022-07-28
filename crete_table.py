import os
import psycopg2
import queries
from dotenv import load_dotenv



def create_table(cur,conn):
    for query in queries.create_table_queries:
        cur.execute(query)
        conn.commit()
        

def drop_table_if_exisit(cur,conn):
    for query in queries.drop_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    load_dotenv()
    try:
        conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"))
        print(conn.info)
    except:
        raise(f"error in connection")

    cur = conn.cursor()

    
    drop_table_if_exisit(cur, conn)
    create_table(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()