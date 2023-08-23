import pyodbc, requests, json, sys, pandas, ast
from datetime import datetime as dt
from io import StringIO

# Username and Password placed here for simplicity
server_name = 'localhost'
master_db = 'master'
sec_db = 'secdb'
full_idx_schema = 'index_full'
full_idx_table = 'full_index'
username = 'SA'
password = 'Mypass123456'
url = 'https://www.sec.gov/Archives/edgar/full-index/'


def get_connection_string(db):
    # Create a connection string
    connection_string = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db};UID={username};PWD={password};'
    return connection_string


def connect_to_database(db):
    connection = pyodbc.connect(get_connection_string(db))
    connection.commit()
    return connection


def execute_query(conn, qry, rw):
    cursor = conn.cursor()
    if rw == 'w':
        cursor.execute(qry)
        conn.commit()
        return True
    elif rw == 'r':
        cursor.execute(qry)
        results = cursor.fetchall()
        return results
    else:
        print('PLEASE SPECIFY READ/WRITE FOR SQL QUERY')
        sys.exit()
        
        

def close_db_connection(conn):
    conn.close()
    return True


def create_new_database():
    # CONNECT TO MASTER DATABASE
    connection = connect_to_database(master_db)
    # CREATE NEW DATABASE
    sql_create_db = f"CREATE DATABASE {sec_db};"
    if execute_query(connection, sql_create_db, 'w'):
        # CLOSE CONNECTION
        if close_db_connection(connection):
            return True


def create_new_schema():
    # CONNECT TO NEW DATABASE
    connection = connect_to_database(sec_db)
    # CREATE NEW SCHEMA
    sql_create_schema = f"CREATE SCHEMA {full_idx_schema};"
    if execute_query(connection, sql_create_schema, 'w'):
        # CLOSE CONNECTION
        if close_db_connection(connection):
            return True

    
def create_directory_table(table_name, table_type):
    table_type_list = {
        'dir': f'''
            CREATE TABLE {full_idx_schema}.{table_name} (
                last_modified DATETIME,
                name NVARCHAR(255),
                type NVARCHAR(50),
                href NVARCHAR(255),
                size NVARCHAR(50),
                insert_date DATETIME,
                is_deleted BIT DEFAULT 0
            );
        ''',
        'idx': f'''
            CREATE TABLE {full_idx_schema}.{table_name} (
                company_name NVARCHAR(255),
                form_type NVARCHAR(50),
                cik NVARCHAR(10),
                date_filed DATE,
                url NVARCHAR(100)
            );
        '''
    }

    connection = connect_to_database(sec_db)
    sql_create_table = table_type_list[table_type]
    if execute_query(connection, sql_create_table, 'w'):
        if close_db_connection(connection):
            return True

def create_full_index_and_schema():
    if create_new_database():
        if create_new_schema():
            return True


            
def format_date_time(date_time):
    current_format = '%m/%d/%Y %I:%M:%S %p'
    new_format = '%Y-%m-%d %H:%M:%S'
    formatted_date_time = dt.strptime(date_time, current_format).strftime(new_format)
    return formatted_date_time
  

def send_web_request(web_url, file_type):
    headers = {
        'User-Agent': 'Deny_Tran tran.deny@outlook.com',
        'Accept-Encoding': 'gzip, deflate',
        'Accept': file_type,
        'Host': 'www.sec.gov'
    }

    response = requests.get(web_url, headers=headers)
    if file_type == 'json':
        if response.status_code == 200:
            try:
                response_data = response.json()
                directory_items = response_data['directory']['item']
                return directory_items
            except json.JSONDecodeError as e:
                print(f"Error decoding {file_type}: {e}")
        else:
            print(f"Failed to fetch data from the URL {web_url}.\nStatus code: {response.status_code}")
    else:
        return response.content
            
            
            
            
def extract_json_data(json_data):
    last_modified = format_date_time(json_data['last-modified'])
    name = json_data['name']
    item_type = json_data['type']
    href = json_data['href']
    size = json_data['size']
    return [last_modified, name, item_type, href, size]


def request_and_insert_directory_table_data(web_url, data_type, tbl):
    directory_items = send_web_request(web_url, data_type)

    for item in directory_items:
        try:
            connection = connect_to_database(sec_db)
            
            current_datetime = dt.now().replace(microsecond=0)
            data = extract_json_data(item) + [current_datetime]
            data_string = ', '.join([f"'{i}'" for i in data])

            sql_insert_data = f"""
                INSERT INTO {full_idx_schema}.{tbl} (
                    last_modified, name, type, href, size, 
                    insert_date, is_deleted)
                VALUES ({data_string}, 0);
            """
            if execute_query(connection, sql_insert_data, 'w'):
                close_db_connection(connection)
        except pyodbc.Error as e:
            print("A database error occurred:", e)



def import_quarterly_dirs():
    sql_annual_dirs = f"""
        SELECT
            href
        FROM
            {full_idx_schema}.{full_idx_table}
        WHERE
            type = 'dir'
    """
    connection = connect_to_database(sec_db)
    qtr_dirs_list = ''.join([
        f'"{i[0]}QTR1/", "{i[0]}QTR2/", "{i[0]}QTR3/", "{i[0]}QTR4/", ' 
        for i in execute_query(connection, sql_annual_dirs, 'r')
    ])

    qtr_dirs_list = [url+i+'index.json' for i in ast.literal_eval(f'[{qtr_dirs_list}]')]

    for qtr in qtr_dirs_list:
        split_list = qtr.split('/')
        quarter = split_list[-2]
        year = split_list[-3]
        tbl = f'full_index_{year}_{quarter}'.lower()
        if create_directory_table(tbl, 'dir'):
            request_and_insert_directory_table_data(qtr, 'application/json', tbl)






def bulk_insert_data(data_list, connection, schema, table):
    insert_query = f'''INSERT INTO {schema}.{table} (company_name, form_type, cik, date_filed, url)
                      VALUES (?, ?, ?, ?, ?);'''

    cursor = connection.cursor()
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        print(f'{table}: bulk insert successful.')
    except pyodbc.Error as e:
        connection.rollback()
        print(f'{table}: a database error occurred - ', e)
    finally:
        cursor.close()


def import_quarterly_crawler_idx():
    sql_crawler_idx = f'''
        SELECT name
        FROM sys.tables
        WHERE name LIKE 'full_index_[0-9][0-9][0-9][0-9]_qtr[0-9]';
    '''
    connection = connect_to_database(sec_db)

    sql_crawler_idx = execute_query(connection, sql_crawler_idx, 'r')
    idx_list = list(zip([i[0] + '_crawler_idx' for i in sql_crawler_idx], [url + i[0].replace('_', '/').replace('/qtr', '/QTR').split('x/')[-1] + '/crawler.idx' for i in sql_crawler_idx]))

    print(idx_list)

    connection.close()

    for idx in idx_list:
        connection = connect_to_database(sec_db)
        req = idx[-1]
        tbl = idx[0]

        try:
            create_directory_table(tbl, 'idx')
            print(f'Table {tbl} created.')
        except Exception:
            print(f'Unable to create {tbl}.')
            continue

        print(f'Processing request for {tbl}.')
        file_content = send_web_request(req, 'text/plain')

        # Convert bytes to a string and split into lines
        file_lines = file_content.decode('utf-8', errors='replace').splitlines()
        headers_list = ['Company Name', 'Form Type', 'CIK', 'Date Filed', 'URL']
        header_line = 0
        for line in range(len(file_lines)):
            if file_lines[line].startswith('Company Name'):
                header_line = line
                break
        hdr_lines = file_lines[header_line]
        cols = [hdr_lines.find(hdr) for hdr in headers_list]
        max_len = max([len(line) for line in file_lines])
        for col in range(len(cols)):
            if col == len(cols) - 1:
                cols[col] = [cols[col], max_len]
            else:
                cols[col] = [cols[col], cols[col + 1] - 1]
        cols = [tuple(col) for col in cols]
        file_lines = file_lines[header_line + 2:]
        col_specs = [(start, end) for start, end in cols]
        file_lines = StringIO('\n'.join(file_lines))
        df = pandas.read_fwf(file_lines, colspecs=col_specs, header=None)
        df.columns = ['company_name', 'form_type', 'cik', 'date_filed', 'url']
        df = df.apply(lambda c: c.astype(str).str.replace("'", "''"))
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        data_list = df.values.tolist()
        if data_list:
            try:
                connection = connect_to_database(sec_db)
                bulk_insert_data(data_list, connection, full_idx_schema, tbl)
            except pyodbc.Error as e:
                print("A database connection error occurred:", e)
            finally:
                if connection:
                    close_db_connection(connection)


def create_unique_quarterly_cik_list():
    sql_idx_qry = """
        SELECT name
        FROM sys.tables
        WHERE name LIKE '%crawler_idx';
    """
    connection = connect_to_database(sec_db)
    for j in [i[0] for i in execute_query(connection, sql_idx_qry, 'r')]:
        table_name = j.replace('crawler_idx', 'cik_list')
        sql_cik_qry = f"""
            SELECT DISTINCT
                CAST(cik AS INTEGER) AS cik, 
                company_name
            INTO 
                    index_full.{table_name}
            FROM
                index_full.{j}
            ORDER BY
                cik;
        """
        sql_cik_qry = execute_query(connection, sql_cik_qry, 'w')
        if sql_cik_qry: pass
        else: sys.exit()



def create_unique_cik_list():
    sql = """
        SELECT name
        FROM sys.tables
        WHERE name LIKE '%cik_list';
    """
    connection = connect_to_database(sec_db)
    table_names = [i[0] for i in execute_query(connection, sql, 'r')]
    union_query = ''
    for table_name in table_names:
        ys = 11
        ye = ys + 4
        qtr = ye + 4
        idx = int(table_name[ys:ye]) + (int(table_name[qtr]) / 10)
        union_query += f"SELECT {idx} AS Idx, * FROM index_full.{table_name} UNION ALL "

    # Remove the trailing UNION ALL
    union_query = union_query[:-11]

    # Execute the query
    final_query = f"""
        SELECT * INTO index_full.combined_results
        FROM (
            {union_query}
        ) AS CombinedData
        ORDER BY Idx
    """
    execute_query(connection, final_query, 'w')
    create_unique_table_query = f"""
        WITH cik_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cik ORDER BY Idx DESC) AS rn
            FROM index_full.combined_results
        )
        SELECT cik, company_name INTO index_full.cik_list
        FROM cik_data
        WHERE rn = 1
        ORDER BY cik;
    """

    # Execute the query to create the new table with unique rows
    execute_query(connection, create_unique_table_query, 'w')

    # Execute the query to drop the table
    drop_table_query = "DROP TABLE IF EXISTS index_full.combined_results;"
    execute_query(connection, drop_table_query, 'w')

def main():
    if create_full_index_and_schema():
        create_directory_table('full_idx', 'dir')

    request_and_insert_directory_table_data(f'{url}index.json', 'json', full_idx_table)

    import_quarterly_dirs()

    import_quarterly_crawler_idx()

    create_unique_quarterly_cik_list()

    create_unique_cik_list()


if __name__ == "__main__":
    main()