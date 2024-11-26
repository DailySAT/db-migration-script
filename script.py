import csv
import psycopg2

# Corrected connection string
CONNECTION_STRING = ""

# CSV file path
CSV_FILE = 'FILE.csv'

# Target table name in the database
TABLE_NAME = 'NAME'

def insert_row(cursor, table_name, row):
    """
    Insert a row into the specified table.
    Handle None values by replacing them with empty strings or appropriate defaults.
    """
    # Replace None values with empty strings or 'N/A'
    row = {key: (value if value is not None else 'N/A') for key, value in row.items()}

    # Prepare the column names and placeholders
    columns = ', '.join(row.keys())
    values_placeholder = ', '.join(['%s'] * len(row))

    # Create the insert query
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
    
    # Execute the query
    cursor.execute(query, list(row.values()))

def main():
    connection = None
    cursor = None
    try:
        # Connect to PostgreSQL database using the connection string
        connection = psycopg2.connect(CONNECTION_STRING)
        cursor = connection.cursor()
        print("Connected to the database.")
        
        # Open and read the CSV file
        with open(CSV_FILE, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if (row == "id"):
                    continue
                insert_row(cursor, TABLE_NAME, row)
        
        # Commit the transaction
        connection.commit()
        print("Data inserted successfully.")

    except psycopg2.OperationalError as error:
        print(f"Database connection error: {error}")
    except Exception as error:
        print(f"An error occurred: {error}")
        if connection:
            connection.rollback()

    finally:
        # Close the database connection and cursor
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
