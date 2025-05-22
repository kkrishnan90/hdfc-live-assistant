import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv() 

# Configuration
PROJECT_ID = "account-pocs"
DATASET_ID = "bank_voice_assistant_dataset"
LOCATION = "US"
# SQL_FILE_PATH is relative to the workspace root, where the script is expected to be run from.
SQL_FILE_PATH = "./bigquery_setup.sql"

def create_dataset_if_not_exists(client: bigquery.Client, dataset_id: str, location: str):
    """Creates a BigQuery dataset if it does not already exist."""
    full_dataset_id = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(full_dataset_id)
        print(f"Dataset {full_dataset_id} already exists.")
    except NotFound:
        print(f"Dataset {full_dataset_id} not found. Creating dataset...")
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = location
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"Successfully created dataset {full_dataset_id} in location {location}.")
        except Exception as e:
            print(f"Error creating dataset {full_dataset_id}: {e}")
            raise
    except Exception as e:
        print(f"Error checking dataset {full_dataset_id}: {e}")
        raise


def execute_sql_from_file(client: bigquery.Client, sql_file_path: str, project_id: str, dataset_id: str):
    """Reads SQL from a file, replaces placeholders, and executes statements."""
    print(f"\nAttempting to read SQL file from: {os.path.abspath(sql_file_path)}")
    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        print(f"Successfully read SQL file: {sql_file_path}")
    except FileNotFoundError:
        print(f"Error: SQL file not found at {sql_file_path} (abs path: {os.path.abspath(sql_file_path)})")
        print(f"Current working directory: {os.getcwd()}")
        raise
    except Exception as e:
        print(f"Error reading SQL file {sql_file_path}: {e}")
        raise

    try:
        # Replace placeholders
        sql_content = sql_content.replace("{{PROJECT_ID}}", project_id)
        sql_content = sql_content.replace("{{DATASET_ID}}", dataset_id)

        # Split into individual statements
        # Handles statements separated by ';' and filters out empty ones.
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

        if not statements:
            print(f"No SQL statements found in {sql_file_path} after processing.")
            return

        print(f"\nExecuting {len(statements)} SQL statement(s) from {sql_file_path}:")
        for i, statement in enumerate(statements):
            print(f"\nExecuting statement {i+1}/{len(statements)}:")
            # Print first few characters of statement for brevity in logs
            print(f"SQL: {statement[:200]}{'...' if len(statement) > 200 else ''}")
            try:
                query_job = client.query(statement)
                query_job.result()  # Wait for the job to complete
                print(f"Successfully executed statement {i+1}.")
            except Exception as e:
                print(f"Error executing statement {i+1}: {e}")
                print(f"Failed SQL: {statement}")
                raise  # Re-raise to stop execution on first error
        print("\nAll SQL statements executed successfully.")

    except Exception as e:
        print(f"An error occurred during SQL processing or execution: {e}")
        raise

if __name__ == "__main__":
    print("Starting BigQuery initialization script...")
    print(f"Expected SQL file path: {SQL_FILE_PATH}")
    print(f"Current working directory: {os.getcwd()}")
    # The script assumes GOOGLE_APPLICATION_CREDENTIALS environment variable is correctly set.
    bq_client = None # Initialize to None for finally block
    try:
        print(f"Initializing BigQuery client for project: {PROJECT_ID}...")
        bq_client = bigquery.Client(project=PROJECT_ID)
        print(f"BigQuery client initialized successfully for project {PROJECT_ID}.")

        create_dataset_if_not_exists(bq_client, DATASET_ID, LOCATION)
        execute_sql_from_file(bq_client, SQL_FILE_PATH, PROJECT_ID, DATASET_ID)

        print("\nBigQuery setup completed successfully.")
    except NotFound as nf_error:
        print(f"\nA 'Not Found' error occurred: {nf_error}")
        print("This might be due to incorrect project ID, dataset ID, or permissions issues.")
        print("BigQuery setup failed.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the BigQuery setup process: {e}")
        print("BigQuery setup failed.")
    finally:
        if bq_client:
            # No explicit close needed for bigquery.Client usually,
            # but good practice if there were closable resources.
            pass
        print("BigQuery initialization script finished.")