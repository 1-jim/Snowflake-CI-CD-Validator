import os
from pathlib import Path
import re
import sys
import duckdb
import sqlparse
import snowflake.connector

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

from jinja2 import Environment, FileSystemLoader, Template
from dotenv import load_dotenv

# Iterates through Snowflake subdirectories, executing against all .sql files with the following exceptions;
folder_exceptions_list = ['Scripts', 'Tasks']

def getSnowflakeConn():
    phrase = os.getenv('PRIVATE_KEY_PASSPHRASE')
    rsa_key_path = os.getenv('RSA_KEY_PATH', 'rsa_key.p8')
    print(f'Key path is: {rsa_key_path}')     
    with open(rsa_key_path, "rb") as key:
        p_key= serialization.load_pem_private_key(
        key.read(),
        password=phrase.encode(),
        backend=default_backend()
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    connection = snowflake.connector.connect(
    user=os.getenv('SF_USER'),
    private_key=pkb,
    account=os.getenv('SF_ACCOUNT'),
    warehouse=os.getenv('SF_WAREHOUSE'),
    database=os.getenv('SF_DATABASE'),
    schema=os.getenv('SF_SCHEMA')
    )
    return connection    

def create_error_table(conn):
    conn.execute("DROP TABLE IF EXISTS errors")
    conn.execute("CREATE TABLE errors (error_message STRING)")

def insert_error(conn, error_message):
    conn.execute(f"INSERT INTO errors VALUES (?)", [error_message])
    print(f'Error logged locally: {error_message}')

def find_sql_files(directory):
    suffix = '.sql'
    sql_files = []
    print(f'Iterating through directory: {directory}')
    
    # Convert the provided directory path to a Path object
    directory_path = Path(directory)

    # Check if the directory exists
    if not directory_path.exists() or not directory_path.is_dir():
        print(f"Error: Directory '{directory}' not found or is not a directory.")
        return sql_files

    # Iterate through the directory recursively
    for sub_directory in directory_path.rglob('*'):
        # Check if it's a file and ends with '.sql'
        if sub_directory.is_file() and sub_directory.suffix.lower() == suffix:
            # Exclude directories in the exceptions list
            if sub_directory.parent.name in folder_exceptions_list:
                continue
            else:
                sql_files.append(sub_directory)

    print(f'There are {len(sql_files)} SQL files')
    return sql_files
      
def remove_comments(sql_statement):
    # Remove single-line -- comments
    sql_statement = re.sub(r'--.*', '', sql_statement)
    # Remove multi-line /* */ comments
    sql_statement = re.sub(r'/\*.*?\*/', '', sql_statement, flags=re.DOTALL)
    return sql_statement

def explain_sql_files(root, target_dir, localDbConn):
    print(f'Connecting to Default Snowflake Schema: {target_dir}..')
    conn = getSnowflakeConn()
    
    sql_files_list = find_sql_files(os.path.join(root, target_dir))            
    for sql_file in sql_files_list:
        with open(sql_file, "r") as file:
            raw_sql_content = file.read()
            
            print(f"Found SQL file: {sql_file}")
            sql_content = render_jinja_template(raw_sql_content, jinja_context)
            
            if not sql_content.upper().startswith('CREATE') and 'JAVASCRIPT' in sql_content:
                continue
            print(f'running statements in file: {sql_file}')
            sql_statements = sqlparse.split(sql_content)
            
            for sql_statement in sql_statements:
                sql_statement = remove_comments(sql_statement)
                if sql_statement:  # Check if statement is not empty
                    if sql_statement.startswith('USE SCHEMA'):
                        try:
                            cursor = conn.cursor()
                            cursor.execute(sql_statement)
                            cursor.close()
                        except snowflake.connector.errors.ProgrammingError as e:
                            print(f'error identified with Schema in statement: {sql_statement}')
                            log_error_file = os.path.splitext(sql_file)[0] + "_error.log"
                            errorMsg = f"SCHEMA ERROR in file {sql_file}:\n{str(e)}\n\n"
                            with open(log_error_file, "a") as error_file:
                                error_file.write(errorMsg)
                            insert_error(localDbConn, errorMsg)

                    elif '{{ environment }}' not in sql_statement.strip().upper():
                        sql_statement = sql_statement.replace('{{sourcedbname}}','EMH_DEV') #need to implement Jinja scripting for EXPLAIN tool.
                        sql_statement = f"EXPLAIN {sql_statement}"
                        try:
                            # Execute SQL query
                            cursor = conn.cursor()
                            cursor.execute(sql_statement)
                            cursor.close()
                        except snowflake.connector.errors.ProgrammingError as e:
                            print(f'error identified with Schema in file: {sql_file}')
                            # Log the error message
                            log_error_file = os.path.splitext(sql_file)[0] + "_error.log"
                            error_msg = f"Error executing SQL statement from file {sql_file}:\n{str(e)}\n\n"
                            with open(log_error_file, "a") as error_file:
                                error_file.write(error_msg)
                            insert_error(localDbConn, error_msg)

    conn.close()

def get_errors(db_conn):
    return db_conn.execute("SELECT error_message FROM errors").fetchall()

def render_jinja_template(content, context):
    template = Template(content)
    rendered_sql = template.render(context)
    return rendered_sql

if __name__ == "__main__":
    load_dotenv()
        
    # Define your Jinja context variables here
    jinja_context = {
        
        'environment': os.getenv('ENVIRONMENT'),
        'dbname': os.getenv('SF_DATABASE')
    }

    db = os.getenv('SF_DATABASE')
    root = os.getenv('SF_CODE_FOLDER', './snowflake/') #+ db
    dir = os.getenv('SF_SCHEMA')

    with duckdb.connect('errors.db') as db_conn:
        create_error_table(db_conn)
        # test_teams_message(db_conn)        
        explain_sql_files(root, dir, db_conn)
        errors = get_errors(db_conn)
        if len(errors) > 0:
            # Extract error messages from tuples and join them
            error_messages = [error[0] for error in errors]
            for msg in error_messages:
                sys.stderr.write(msg)
        else:
            msg = (f'Successfully validated selected schema: {dir}')
            print(msg)