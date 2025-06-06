import os
from google.cloud import bigquery
import datetime
import uuid
import logging
import sys # Added to redirect logger to stdout
import json # For structured logging of parameters and results
from dotenv import load_dotenv





# Load environment variables from .env file
load_dotenv()

# Configure logging
# Ensure a logger instance is used for more control if needed, but basicConfig is fine for now.
# For file-based logging, a FileHandler could be added here.
# For now, stdout logging as configured is acceptable per instructions.
logging.basicConfig(
    stream=sys.stdout, # Direct logs to stdout
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Use a specific logger for this module

# Global store for logs
GLOBAL_LOG_STORE = []
 
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")

# Initialize BigQuery Client
# Ensure GOOGLE_APPLICATION_CREDENTIALS environment variable is set.
# For local development, you might set it like this:
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-file.json"
# In a deployed environment (e.g., Google Cloud Run/Functions), this is often handled automatically.
try:
    # client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT, credentials=service_account.Credentials.from_service_account_file(CREDENTIALS_PATH))
    client = bigquery.Client(project="account-pocs")
    # Attempt a simple query to verify connection and credentials
    # This also helps in resolving the project ID if not explicitly set
    if client.project:
        logger.info("\033[92mBigQuery client initialized successfully for project: %s.\033[0m", client.project)
    else: # Should not happen if client init is successful without error
        logger.warning("BigQuery client initialized, but project ID could not be determined automatically.")
    # client.query("SELECT 1").result() # Optional: verify with a query
except Exception as e:
    logger.error("Failed to initialize BigQuery client: %s", e, exc_info=True) # Added exc_info=True
    # Fallback or raise an error if the client is essential for the module to load
    client = None

# Placeholder for User ID - replace with actual authentication mechanism later
USER_ID = "user_krishnan_001"

# Determine Project ID and Dataset ID
# Use GOOGLE_CLOUD_PROJECT env var if set, otherwise try to get from initialized client
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID and client:
    PROJECT_ID = client.project
if not PROJECT_ID:
    logger.warning("GOOGLE_CLOUD_PROJECT environment variable not set and client.project is unavailable. Using placeholder 'your-gcp-project-id'. Table references might be incorrect.")
    PROJECT_ID = "your-gcp-project-id" # Fallback placeholder

DATASET_ID = "bank_voice_assistant_dataset" # Assuming the dataset name from bigquery_setup.sql

# --- Structured Logging Helper ---
def log_bq_interaction(func_name: str, params: dict, query: str = None, status: str = "N/A", result_summary: str = None, error_message: str = None):
    """Helper function for structured logging of BigQuery interactions."""
    log_entry = {
        "operation": func_name,
        "parameters": params,
        "query": query if query else "N/A",
        "status": status,
    }
    if result_summary is not None: # Could be a success message or data summary
        log_entry["result_summary"] = result_summary
    if error_message:
        log_entry["error_message"] = error_message
    
    # Using logger.info for all structured logs for simplicity,
    # the 'status' field within the JSON will indicate success/failure.
    # Alternatively, use logger.error for failed statuses.
    
    # Store the log entry in the global list
    GLOBAL_LOG_STORE.append(log_entry)

    if "ERROR" in status.upper() or "FAIL" in status.upper():
        logger.error(json.dumps(log_entry)) # Error logs remain default color
    else:
        logger.info("\033[92m%s\033[0m", json.dumps(log_entry)) # Successful BQ interactions in green

# Helper to construct full table IDs
def _table_ref(table_name: str) -> str:
    if PROJECT_ID == "your-gcp-project-id": # Check if using placeholder
        # This is a less safe fallback if project ID couldn't be determined
        logger.warning("Using fallback table reference for %s as PROJECT_ID is a placeholder.", table_name)
        return f"`{DATASET_ID}.{table_name}`"
    return f"`{PROJECT_ID}.{DATASET_ID}.{table_name}`"

def test_bigquery_connection():
    """
    Tests the BigQuery connection by executing a simple query.
    Logs success or failure.
    """
    func_name = "test_bigquery_connection"
    params = {}
    # Using a very simple query that doesn't rely on specific tables initially
    query_str = "SELECT 1 AS test_column"
    logger.info("[%s] Attempting to test BigQuery connection.", func_name)

    if not client:
        log_message = "BigQuery client is not initialized. Cannot perform connection test."
        logger.error("[%s] %s", func_name, log_message)
        # Manual log for consistency if needed
        GLOBAL_LOG_STORE.append({
            "operation": func_name, "parameters": params, "query": query_str,
            "status": "ERROR_CLIENT_NOT_INITIALIZED", "error_message": log_message
        })
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": log_message}

    try:
        logger.info("\033[92m[%s] Executing test query: %s\033[0m", func_name, query_str)
        query_job = client.query(query_str)
        results = query_job.result()  # Waits for the job to complete.
        
        data_val = None
        for row in results:
            data_val = row.test_column # Access the aliased column
            break

        result_summary = f"Test query successful. Result: {data_val}"
        logger.info("\033[92m[%s] %s\033[0m", func_name, result_summary)
        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=result_summary)
        return {"status": "SUCCESS", "message": result_summary, "data": data_val}
    except Exception as e:
        error_message = f"BigQuery connection test failed: {str(e)}"
        # Log with full traceback here
        logger.error("[%s] %s", func_name, error_message, exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=error_message)
        return {"status": "ERROR_QUERY_FAILED", "message": error_message}

def _get_account_details(account_type: str, user_id: str) -> dict:
    """
    Helper function to retrieve account_id, balance, and currency for a given account_type and user_id.
    """
    func_name = "_get_account_details"
    params = {"account_type": account_type, "user_id": user_id}
    query_str = None
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    accounts_table = _table_ref("Accounts")
    query_str = f"""
        SELECT account_id, balance, currency
        FROM {accounts_table}
        WHERE user_id = @user_id AND account_type = @account_type
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("account_type", "STRING", account_type),
        ]
    )
    try:
        query_job = client.query(query_str, job_config=job_config)
        results = query_job.result()
        row_data = None
        for row in results: # Should be at most one row due to LIMIT 1
            row_data = {
                "account_id": row.account_id,
                "balance": float(row.balance),
                "currency": row.currency,
                "account_type": account_type
            }
            break # Found the account
        
        if row_data:
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Account found: {row_data['account_id']}")
            return {"status": "SUCCESS", **row_data}
        else:
            log_bq_interaction(func_name, params, query_str, status="ERROR_ACCOUNT_NOT_FOUND", error_message=f"Account type '{account_type}' not found for user '{user_id}'.")
            return {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": f"Account type '{account_type}' not found for user '{user_id}'."}
    except Exception as e:
        logger.error("Exception details in %s: %s", func_name, str(e), exc_info=True) # Added exc_info=True
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        # The original logging.error is now covered by log_bq_interaction if status is error
        return {"status": "ERROR_QUERY_FAILED", "message": str(e)}


def get_account_balance(account_type: str) -> dict:
    """
    Queries the Accounts table for the balance of a specific account type for the USER_ID.
    Returns:
        dict: {"account_type": "checking", "balance": 1250.75, "currency": "USD", "account_id": "acc_chk_krishnan_001"}
              or an error message.
    """
    details = _get_account_details(account_type, USER_ID)
    if details["status"] == "SUCCESS":
        return {
            "account_type": account_type, # Already in details, but spec asks for it explicitly
            "balance": details["balance"],
            "currency": details["currency"],
            "account_id": details["account_id"]
        }
    return details # Return the error message from helper


def get_transaction_history(account_type: str, limit: int = 5) -> list:
    """
    Fetches transaction history for a given account_type for the default USER_ID.
    """
    func_name = "get_transaction_history"
    params = {"account_type": account_type, "limit": limit, "user_id": USER_ID}
    query_str = None

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return [{"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}]

    # _get_account_details already logs its interaction
    account_details = _get_account_details(account_type, USER_ID)
    if account_details["status"] != "SUCCESS":
        # Log this specific failure context for get_transaction_history
        log_bq_interaction(func_name, params, status=account_details["status"], error_message=f"Failed to get account details for {account_type}: {account_details.get('message')}")
        return [account_details]

    account_id = account_details["account_id"]
    transactions_table = _table_ref("Transactions")

    query_str = f"""
        SELECT transaction_id, date, description, amount, currency, type
        FROM {transactions_table}
        WHERE account_id = @account_id
        ORDER BY date DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    try:
        query_job = client.query(query_str, job_config=job_config)
        results = query_job.result()
        transactions_data = []
        for row in results:
            transactions_data.append({
                "transaction_id": row.transaction_id,
                "date": row.date.isoformat() if isinstance(row.date, (datetime.datetime, datetime.date)) else str(row.date),
                "description": row.description,
                "amount": float(row.amount),
                "currency": row.currency,
                "type": row.type
            })
        
        if not transactions_data:
            log_bq_interaction(func_name, params, query_str, status="NO_TRANSACTIONS_FOUND", result_summary=f"No transactions found for account {account_id}.")
            return [{"status": "NO_TRANSACTIONS_FOUND", "message": f"No transactions found for account {account_id} (type: {account_type})."}]
        
        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Retrieved {len(transactions_data)} transaction(s).")
        return transactions_data
    except Exception as e:
        logger.error("Exception details in %s: %s", func_name, str(e), exc_info=True) # Added exc_info=True
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return [{"status": "ERROR_QUERY_FAILED", "message": str(e)}]


def initiate_fund_transfer_check(from_account_type: str, to_account_type: str, amount: float) -> dict:
    """
    Checks if a fund transfer is possible between two account types for the USER_ID.
    """
    func_name = "initiate_fund_transfer_check"
    params = {"from_account_type": from_account_type, "to_account_type": to_account_type, "amount": amount, "user_id": USER_ID}

    if not isinstance(amount, (int, float)) or amount <= 0:
        log_bq_interaction(func_name, params, status="ERROR_INVALID_AMOUNT", error_message="Transfer amount must be a positive number.")
        return {"status": "ERROR_INVALID_AMOUNT", "message": "Transfer amount must be a positive number."}

    # _get_account_details logs its own interactions
    from_account_details = _get_account_details(from_account_type, USER_ID)
    if from_account_details["status"] != "SUCCESS":
        err_msg = f"From account ('{from_account_type}'): {from_account_details.get('message', 'Error fetching details.')}"
        log_bq_interaction(func_name, params, status=from_account_details["status"], error_message=err_msg)
        return {"status": from_account_details["status"], "message": err_msg}

    to_account_details = _get_account_details(to_account_type, USER_ID)
    if to_account_details["status"] != "SUCCESS":
        err_msg = f"To account ('{to_account_type}'): {to_account_details.get('message', 'Error fetching details.')}"
        log_bq_interaction(func_name, params, status=to_account_details["status"], error_message=err_msg)
        return {"status": to_account_details["status"], "message": err_msg}

    from_account_id = from_account_details["account_id"]
    from_balance = from_account_details["balance"]
    to_account_id = to_account_details["account_id"]

    if from_account_id == to_account_id:
        log_bq_interaction(func_name, params, status="ERROR_SAME_ACCOUNT", error_message="Cannot transfer funds to the same account ID.")
        return {"status": "ERROR_SAME_ACCOUNT", "message": "Cannot transfer funds to the same account type, resulting in the same account ID."}
    
    result_data = {}
    if from_balance >= amount:
        status = "SUFFICIENT_FUNDS"
        result_data = {
            "from_account_id": from_account_id, "to_account_id": to_account_id,
            "from_account_balance": from_balance, "transfer_amount": amount,
            "currency": from_account_details["currency"]
        }
        log_bq_interaction(func_name, params, status=status, result_summary=f"Sufficient funds. From: {from_account_id}, To: {to_account_id}, Amount: {amount}")
    else:
        status = "INSUFFICIENT_FUNDS"
        result_data = {
            "current_balance": from_balance, "from_account_id": from_account_id,
            "to_account_id": to_account_id, "requested_amount": amount,
            "currency": from_account_details["currency"]
        }
        log_bq_interaction(func_name, params, status=status, error_message=f"Insufficient funds. Has: {from_balance}, Needs: {amount}")
    
    return {"status": status, **result_data}


def execute_fund_transfer(from_account_id: str, to_account_id: str, amount: float, currency: str, memo: str) -> dict:
    """
    Executes a fund transfer by updating account balances and recording transactions in BigQuery.
    Operations are performed within a multi-statement transaction for atomicity.
    """
    func_name = "execute_fund_transfer"
    params = {"from_account_id": from_account_id, "to_account_id": to_account_id, "amount": amount, "currency": currency, "memo": memo, "user_id": USER_ID}
    query_str = None # Will hold the multi-statement query

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    if not isinstance(amount, (int, float)) or amount <= 0:
        log_bq_interaction(func_name, params, status="ERROR_INVALID_AMOUNT", error_message="Transfer amount must be a positive number.")
        return {"status": "ERROR_INVALID_AMOUNT", "message": "Transfer amount must be a positive number."}

    if from_account_id == to_account_id:
        log_bq_interaction(func_name, params, status="ERROR_SAME_ACCOUNT", error_message="Cannot execute transfer to the same account ID.")
        return {"status": "ERROR_SAME_ACCOUNT", "message": "Cannot transfer funds to the same account."}

    # Fetch account details for validation
    from_account_details = get_account_balance_by_id(from_account_id, USER_ID)
    if from_account_details["status"] != "SUCCESS":
        err_msg = f"Sender account '{from_account_id}' not found or error: {from_account_details.get('message')}"
        log_bq_interaction(func_name, params, status="ERROR_FROM_ACCOUNT_INVALID", error_message=err_msg)
        return {"status": "ERROR_FROM_ACCOUNT_INVALID", "message": err_msg}

    to_account_details = get_account_balance_by_id(to_account_id, USER_ID)
    if to_account_details["status"] != "SUCCESS":
        err_msg = f"Recipient account '{to_account_id}' not found or error: {to_account_details.get('message')}"
        log_bq_interaction(func_name, params, status="ERROR_TO_ACCOUNT_INVALID", error_message=err_msg)
        return {"status": "ERROR_TO_ACCOUNT_INVALID", "message": err_msg}

    if from_account_details["currency"] != currency or to_account_details["currency"] != currency:
        err_msg = (f"Currency mismatch. Transfer currency: {currency}, "
                   f"Sender account ({from_account_id}) currency: {from_account_details['currency']}, "
                   f"Recipient account ({to_account_id}) currency: {to_account_details['currency']}.")
        log_bq_interaction(func_name, params, status="ERROR_CURRENCY_MISMATCH", error_message=err_msg)
        return {"status": "ERROR_CURRENCY_MISMATCH", "message": err_msg}

    if from_account_details["balance"] < amount:
        err_msg = f"Insufficient funds in sender account '{from_account_id}'. Has: {from_account_details['balance']} {currency}, Needs: {amount} {currency}"
        log_bq_interaction(func_name, params, status="ERROR_INSUFFICIENT_FUNDS", error_message=err_msg)
        return {
            "status": "ERROR_INSUFFICIENT_FUNDS", "current_balance": from_account_details['balance'],
            "requested_amount": amount, "currency": currency,
            "from_account_id": from_account_id, "to_account_id": to_account_id, "message": err_msg
        }

    transaction_base_id = f"txn_{uuid.uuid4().hex}"
    debit_transaction_id = f"{transaction_base_id}_D"
    credit_transaction_id = f"{transaction_base_id}_C"
    current_timestamp_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    accounts_table = _table_ref("Accounts")
    transactions_table = _table_ref("Transactions")

    # Multi-statement transaction
    # Note: Parameterization within a single multi-statement string sent to client.query()
    # can be tricky. For complex cases, consider stored procedures or multiple client.query calls
    # managed by application logic if BEGIN/COMMIT isn't directly parameterizable as one string.
    # However, for this structure, we'll build the DML string.
    # Ensure values are properly escaped or use query parameters if supported by the client library for multi-statement.
    # For direct DML string construction, ensure numeric types are not quoted, strings are.
    # BigQuery's standard SQL client.query() with @params should handle this.

    query_str = f"""
    BEGIN TRANSACTION;

    -- Decrement sender's balance
    UPDATE {accounts_table}
    SET balance = balance - @amount
    WHERE account_id = @from_account_id AND user_id = @user_id;

    -- Increment recipient's balance
    UPDATE {accounts_table}
    SET balance = balance + @amount
    WHERE account_id = @to_account_id AND user_id = @user_id;

    -- Insert debit transaction for sender
    INSERT INTO {transactions_table} (transaction_id, account_id, user_id, date, description, amount, currency, type, memo)
    VALUES (@debit_transaction_id, @from_account_id, @user_id, @timestamp, @debit_description, -@amount, @currency, 'transfer_debit', @memo);

    -- Insert credit transaction for recipient
    INSERT INTO {transactions_table} (transaction_id, account_id, user_id, date, description, amount, currency, type, memo)
    VALUES (@credit_transaction_id, @to_account_id, @user_id, @timestamp, @credit_description, @amount, @currency, 'transfer_credit', @memo);

    COMMIT TRANSACTION;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("amount", "FLOAT64", amount),
            bigquery.ScalarQueryParameter("from_account_id", "STRING", from_account_id),
            bigquery.ScalarQueryParameter("to_account_id", "STRING", to_account_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", USER_ID),
            bigquery.ScalarQueryParameter("debit_transaction_id", "STRING", debit_transaction_id),
            bigquery.ScalarQueryParameter("credit_transaction_id", "STRING", credit_transaction_id),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", current_timestamp_str),
            bigquery.ScalarQueryParameter("debit_description", "STRING", f"Transfer to account {to_account_id}"),
            bigquery.ScalarQueryParameter("credit_description", "STRING", f"Transfer from account {from_account_id}"),
            bigquery.ScalarQueryParameter("currency", "STRING", currency),
            bigquery.ScalarQueryParameter("memo", "STRING", memo),
        ]
    )

    try:
        logger.info("[%s] Executing fund transfer transaction for user %s from %s to %s for %s %s.", func_name, USER_ID, from_account_id, to_account_id, amount, currency)
        query_job = client.query(query_str, job_config=job_config)
        query_job.result()  # Wait for the transaction to complete

        if query_job.errors:
            # This block might not be reached if errors cause an exception handled by the except block.
            # However, it's good practice to check job.errors if result() doesn't raise.
            error_detail = f"BigQuery transaction failed: {query_job.errors}"
            log_bq_interaction(func_name, params, query_str, status="ERROR_TRANSACTION_FAILED", error_message=error_detail)
            return {"status": "ERROR_TRANSACTION_FAILED", "message": "Fund transfer failed during BigQuery execution.", "details": query_job.errors}

        success_msg = f"Fund transfer of {amount} {currency} from {from_account_id} to {to_account_id} completed successfully. Transaction ID: {transaction_base_id}"
        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=success_msg)
        return {
            "status": "SUCCESS",
            "transaction_id": transaction_base_id,
            "message": success_msg
        }
    except Exception as e:
        error_message = f"Exception during fund transfer: {str(e)}"
        logger.error("[%s] %s", func_name, error_message, exc_info=True)
        # Attempt to rollback if possible, though BigQuery auto-rolls back on error in a transaction
        try:
            client.query("ROLLBACK TRANSACTION;").result() # May error if no transaction active
            logger.info("[%s] Attempted ROLLBACK TRANSACTION due to error.", func_name)
        except Exception as rb_e:
            logger.warning("[%s] Error during explicit ROLLBACK attempt: %s", func_name, rb_e)

        log_bq_interaction(func_name, params, query_str, status="ERROR_EXCEPTION", error_message=error_message)
        return {"status": "ERROR_EXCEPTION", "message": "An internal error occurred during fund transfer.", "details": str(e)}


def get_bill_details(bill_type: str, payee_nickname: str = None) -> dict:
    """
    Queries the RegisteredBillers table for bill details for the USER_ID.
    """
    func_name = "get_bill_details"
    params = {"bill_type": bill_type, "payee_nickname": payee_nickname, "user_id": USER_ID}
    query_str = None

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    billers_table = _table_ref("RegisteredBillers")
    query_params_list = [
        bigquery.ScalarQueryParameter("user_id", "STRING", USER_ID),
    ]
    
    where_conditions = ["user_id = @user_id"]
    
    if bill_type:
        where_conditions.append("bill_type = @bill_type")
        query_params_list.append(bigquery.ScalarQueryParameter("bill_type", "STRING", bill_type))

    if payee_nickname:
        where_conditions.append("payee_nickname = @payee_nickname")
        query_params_list.append(bigquery.ScalarQueryParameter("payee_nickname", "STRING", payee_nickname))
        
    query_str = f"""
        SELECT biller_id, biller_name, last_due_amount as due_amount, last_due_date as due_date, default_payment_account_id
        FROM {billers_table}
        WHERE {" AND ".join(where_conditions)}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=query_params_list)
    
    try:
        query_job = client.query(query_str, job_config=job_config)
        results = list(query_job.result())

        if not results:
            err_msg = f"No billers found for the specified criteria."
            if bill_type: err_msg += f" Type: '{bill_type}'"
            if payee_nickname: err_msg += f" Nickname: '{payee_nickname}'"
            log_bq_interaction(func_name, params, query_str, status="ERROR_BILLER_NOT_FOUND", error_message=err_msg)
            return {"status": "ERROR_BILLER_NOT_FOUND", "message": err_msg}

        billers_data = [{
            "biller_id": row.biller_id,
            "biller_name": row.biller_name,
            "due_amount": float(row.due_amount) if row.due_amount is not None else None,
            "due_date": row.due_date.isoformat() if row.due_date else None,
            "default_payment_account_id": row.default_payment_account_id
        } for row in results]

        result_summary = f"Found {len(billers_data)} biller(s)."
        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=result_summary)
        return {"status": "SUCCESS", "data": billers_data}
    except Exception as e:
        logger.error("Exception in %s: %s", func_name, str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return {"status": "ERROR_QUERY_FAILED", "message": str(e)}


def _get_payee_name(payee_id: str, user_id: str) -> str | None:
    """Helper to fetch payee name from RegisteredBillers for a specific user."""
    func_name = "_get_payee_name"
    params = {"payee_id": payee_id, "user_id": user_id}
    query_str = None

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return None

    billers_table = _table_ref("RegisteredBillers")
    query_str = f"""
        SELECT biller_name FROM {billers_table}
        WHERE biller_id = @payee_id AND user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("payee_id", "STRING", payee_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    try:
        query_job = client.query(query_str, job_config=job_config)
        row = next(query_job.result(), None)
        if row:
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Payee name found: {row.biller_name}")
            return row.biller_name
        else:
            log_bq_interaction(func_name, params, query_str, status="ERROR_PAYEE_NOT_FOUND", error_message="Payee ID not found.")
            return None
    except Exception as e:
        logger.error("Exception in %s: %s", func_name, str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return None


def get_account_balance_by_id(account_id: str, user_id: str) -> dict:
    """Helper to get balance and currency for a specific account_id and user_id."""
    func_name = "get_account_balance_by_id"
    params = {"account_id": account_id, "user_id": user_id}
    query_str = None
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    accounts_table = _table_ref("Accounts")
    query_str = f"""
        SELECT balance, currency FROM {accounts_table}
        WHERE account_id = @account_id AND user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    try:
        query_job = client.query(query_str, job_config=job_config)
        row = next(query_job.result(), None)
        if row:
            result = {"status": "SUCCESS", "balance": float(row.balance), "currency": row.currency}
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Balance found for {account_id}.")
            return result
        else:
            err_msg = f"Account ID '{account_id}' not found for user '{user_id}'."
            log_bq_interaction(func_name, params, query_str, status="ERROR_ACCOUNT_NOT_FOUND", error_message=err_msg)
            return {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": err_msg}
    except Exception as e:
        logger.error("Exception in %s: %s", func_name, str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return {"status": "ERROR_QUERY_FAILED", "message": str(e)}


def pay_bill(amount: float, payee_id: str = None, bill_type: str = None, from_account_id: str = None, user_id: str = None) -> dict:
    """
    Pays a bill, identified by either payee_id or bill_type.
    """
    func_name = "pay_bill"
    user_id = user_id or USER_ID
    params = {"payee_id": payee_id, "bill_type": bill_type, "amount": amount, "from_account_id": from_account_id, "user_id": user_id}

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    if not isinstance(amount, (int, float)) or amount <= 0:
        log_bq_interaction(func_name, params, status="ERROR_INVALID_AMOUNT", error_message="Payment amount must be a positive number.")
        return {"status": "ERROR_INVALID_AMOUNT", "message": "Payment amount must be a positive number."}

    # Resolve biller by type if payee_id is not provided
    if not payee_id and bill_type:
        logger.info("[%s] Attempting to find biller by type: '%s'", func_name, bill_type)
        biller_details = get_bill_details(bill_type=bill_type) # Assuming this finds one biller
        if biller_details.get("status") == "SUCCESS" and biller_details.get("data"):
            payee_id = biller_details["data"][0].get("biller_id")
            logger.info("[%s] Resolved bill_type '%s' to biller_id '%s'.", func_name, bill_type, payee_id)
            params["payee_id"] = payee_id # Update params for logging
        else:
            err_msg = f"Could not find a registered biller for bill type '{bill_type}'."
            log_bq_interaction(func_name, params, status="ERROR_BILLER_NOT_FOUND", error_message=err_msg)
            return {"status": "ERROR_BILLER_NOT_FOUND", "message": err_msg}
    elif not payee_id and not bill_type:
        err_msg = "A biller must be specified by providing either a payee_id or a bill_type."
        log_bq_interaction(func_name, params, status="ERROR_MISSING_BILLER_ID", error_message=err_msg)
        return {"status": "ERROR_MISSING_BILLER_ID", "message": err_msg}

    # Resolve the natural language account name to an account ID first.
    account_details = find_account_by_natural_language(user_id, from_account_id)
    if account_details.get("status") != "SUCCESS":
        err_msg = f"Error with payment account '{from_account_id}': {account_details.get('message')}"
        log_bq_interaction(func_name, params, status=account_details.get("status", "ERROR_ACCOUNT_RESOLUTION_FAILED"), error_message=err_msg)
        return {"status": account_details.get("status", "ERROR_ACCOUNT_RESOLUTION_FAILED"), "message": err_msg}
    
    payment_account_id = account_details.get("account_id")
    logger.info("[%s] Resolved payment account name '%s' to ID '%s'.", func_name, from_account_id, payment_account_id)

    balance_details = get_account_balance_by_id(payment_account_id, user_id)
    if balance_details.get("status") != "SUCCESS":
        err_msg = f"Error with payment account '{from_account_id}': {balance_details.get('message')}"
        log_bq_interaction(func_name, params, status=balance_details.get("status", "ERROR_ACCOUNT_NOT_FOUND"), error_message=err_msg)
        return {"status": balance_details.get("status", "ERROR_ACCOUNT_NOT_FOUND"), "message": err_msg}

    current_balance = balance_details["balance"]
    currency = balance_details["currency"]

    if current_balance < amount:
        err_msg = f"Insufficient funds in account {from_account_id} ({payment_account_id}). Has: {current_balance} {currency}, Needs: {amount} {currency}"
        log_bq_interaction(func_name, params, status="INSUFFICIENT_FUNDS", error_message=err_msg)
        return {
            "status": "INSUFFICIENT_FUNDS", "current_balance": current_balance,
            "requested_amount": amount, "currency": currency,
            "from_account_id": from_account_id, "payee_id": payee_id, "message": err_msg
        }

    payee_name = _get_payee_name(payee_id, user_id)
    if not payee_name:
        err_msg = f"Biller with ID '{payee_id}' not found for user '{user_id}'."
        log_bq_interaction(func_name, params, status="ERROR_BILLER_NOT_FOUND", error_message=err_msg)
        return {"status": "ERROR_BILLER_NOT_FOUND", "message": err_msg}
    
    transaction_id = f"txn_{uuid.uuid4().hex}"
    current_timestamp_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    accounts_table = _table_ref("Accounts")
    transactions_table = _table_ref("Transactions")
    billers_table = _table_ref("RegisteredBillers")

    query_str = f"""
    BEGIN TRANSACTION;

    UPDATE {accounts_table}
    SET balance = balance - @amount
    WHERE account_id = @payment_account_id AND user_id = @user_id;

    INSERT INTO {transactions_table} (transaction_id, account_id, user_id, date, description, amount, currency, type, memo)
    VALUES (@transaction_id, @payment_account_id, @user_id, @timestamp, @description, -@amount, @currency, 'bill_payment', @memo);

    UPDATE {billers_table}
    SET last_due_amount = last_due_amount - @amount
    WHERE biller_id = @payee_id AND user_id = @user_id;

    COMMIT TRANSACTION;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("amount", "FLOAT64", amount),
            bigquery.ScalarQueryParameter("payment_account_id", "STRING", payment_account_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", current_timestamp_str),
            bigquery.ScalarQueryParameter("description", "STRING", f"Bill payment to {payee_name}"),
            bigquery.ScalarQueryParameter("currency", "STRING", currency),
            bigquery.ScalarQueryParameter("memo", "STRING", f"Payment for biller ID {payee_id}"),
            bigquery.ScalarQueryParameter("payee_id", "STRING", payee_id),
        ]
    )

    try:
        logger.info("[%s] Executing bill payment transaction for user %s, payee %s, amount %s %s from account %s.", func_name, user_id, payee_id, amount, currency, payment_account_id)
        query_job = client.query(query_str, job_config=job_config)
        query_job.result()  # Wait for the transaction to complete

        if query_job.errors:
            error_detail = f"BigQuery transaction for bill payment failed: {query_job.errors}"
            log_bq_interaction(func_name, params, query_str, status="ERROR_TRANSACTION_FAILED", error_message=error_detail)
            return {"status": "ERROR_TRANSACTION_FAILED", "message": "Bill payment failed during BigQuery execution.", "details": query_job.errors}

        success_msg = f"Bill payment of {amount} {currency} to {payee_name} from account {from_account_id} was successful. Transaction ID: {transaction_id}."
        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=success_msg)
        return {
            "status": "SUCCESS",
            "transaction_id": transaction_id,
            "biller_id": payee_id,
            "biller_name": payee_name,
            "amount_paid": float(amount),
            "currency": currency,
            "from_account_id": from_account_id,
            "message": success_msg
        }
    except Exception as e:
        error_message = f"Exception during bill payment transaction: {str(e)}"
        logger.error("[%s] %s", func_name, error_message, exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_EXCEPTION", error_message=error_message)
        return {"status": "ERROR_EXCEPTION", "message": "An internal error occurred during bill payment.", "details": str(e)}


def register_biller(user_id: str, biller_name: str, biller_type: str, account_number: str, payee_nickname: str = None, default_payment_account_id: str = None, due_amount: float = None, due_date: str = None) -> dict:
    """
    Registers a new biller for a given user in the RegisteredBillers table.
    Checks for duplicates based on user_id, biller_type, and account_number.
    """
    func_name = "register_biller"
    params = {
        "user_id": user_id, "biller_name": biller_name, "biller_type": biller_type,
        "account_number": account_number, "payee_nickname": payee_nickname,
        "default_payment_account_id": default_payment_account_id,
        "due_amount": due_amount, "due_date": due_date
    }
    query_str_check = None
    query_str_insert = None

    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    # Validate for duplicate biller
    billers_table = _table_ref("RegisteredBillers")
    query_str_check = f"""
        SELECT biller_id FROM {billers_table}
        WHERE user_id = @user_id AND biller_type = @biller_type AND account_number = @account_number
    """
    job_config_check = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("biller_type", "STRING", biller_type),
            bigquery.ScalarQueryParameter("account_number", "STRING", account_number),
        ]
    )
    try:
        existing_biller = list(client.query(query_str_check, job_config=job_config_check).result())
        if existing_biller:
            err_msg = f"A biller with the same type and account number is already registered."
            log_bq_interaction(func_name, params, query_str_check, status="ERROR_DUPLICATE_BILLER", error_message=err_msg)
            return {"status": "ERROR_DUPLICATE_BILLER", "message": err_msg}
    except Exception as e:
        logger.error("Exception during duplicate biller check: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str_check, status="ERROR_QUERY_FAILED", error_message=str(e))
        return {"status": "ERROR_QUERY_FAILED", "message": "Failed to check for duplicate billers."}

    # Generate a unique biller_id
    biller_id = f"biller_{uuid.uuid4().hex}"
    
    # Format due_date if provided
    if due_date:
        try:
            # Assuming due_date is in 'YYYY-MM-DD' format
            due_date_obj = datetime.datetime.strptime(due_date, '%Y-%m-%d').date()
        except ValueError:
            return {"status": "ERROR_INVALID_DATE_FORMAT", "message": "Invalid due_date format. Please use YYYY-MM-DD."}
    else:
        due_date_obj = None

    query_str_insert = f"""
        INSERT INTO {billers_table} (biller_id, user_id, biller_name, bill_type, account_number, payee_nickname, default_payment_account_id, last_due_amount, last_due_date)
        VALUES (@biller_id, @user_id, @biller_name, @biller_type, @account_number, @payee_nickname, @default_payment_account_id, @due_amount, @due_date)
    """
    job_config_insert = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("biller_id", "STRING", biller_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("biller_name", "STRING", biller_name),
            bigquery.ScalarQueryParameter("biller_type", "STRING", biller_type),
            bigquery.ScalarQueryParameter("account_number", "STRING", account_number),
            bigquery.ScalarQueryParameter("payee_nickname", "STRING", payee_nickname),
            bigquery.ScalarQueryParameter("default_payment_account_id", "STRING", default_payment_account_id),
            bigquery.ScalarQueryParameter("due_amount", "FLOAT64", due_amount),
            bigquery.ScalarQueryParameter("due_date", "DATE", due_date_obj),
        ]
    )

    try:
        client.query(query_str_insert, job_config=job_config_insert).result() # Wait for completion
        success_msg = f"Biller '{biller_name}' registered successfully with ID '{biller_id}'."
        log_bq_interaction(func_name, params, query_str_insert, status="SUCCESS", result_summary=success_msg)
        return {"status": "SUCCESS", "biller_id": biller_id, "message": success_msg}
    except Exception as e:
        logger.error("Exception during biller registration: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str_insert, status="ERROR_INSERT_FAILED", error_message=str(e))
        return {"status": "ERROR_INSERT_FAILED", "message": "Failed to register new biller."}


def update_biller_details(user_id: str, payee_id: str, updates: dict) -> dict:
    """
    Updates details for a registered biller.
    """
    func_name = "update_biller_details"
    params = {"user_id": user_id, "payee_id": payee_id, "updates": updates}
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    if not updates:
        return {"status": "NO_UPDATES_PROVIDED", "message": "No updates were provided."}

    billers_table = _table_ref("RegisteredBillers")
    set_clauses = []
    query_params = [
        bigquery.ScalarQueryParameter("biller_id", "STRING", payee_id),
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
    ]

    for key, value in updates.items():
        # Basic validation and type handling
        if key in ["biller_name", "payee_nickname", "default_payment_account_id", "account_number", "bill_type"]:
            param_type = "STRING"
        elif key == "last_due_amount":
            param_type = "FLOAT64"
            value = float(value)
        elif key == "last_due_date":
            param_type = "DATE"
            try:
                value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
            except (ValueError, TypeError):
                return {"status": "ERROR_INVALID_DATE_FORMAT", "message": "Invalid date format for last_due_date. Use YYYY-MM-DD."}
        else:
            continue # Ignore unsupported fields

        set_clauses.append(f"{key} = @{key}")
        query_params.append(bigquery.ScalarQueryParameter(key, param_type, value))

    if not set_clauses:
        return {"status": "NO_VALID_UPDATES", "message": "No valid fields to update were provided."}

    query_str = f"""
        UPDATE {billers_table}
        SET {', '.join(set_clauses)}
        WHERE biller_id = @biller_id AND user_id = @user_id
    """
    
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query_str, job_config=job_config)
        query_job.result() # Wait for completion

        if query_job.num_dml_affected_rows > 0:
            success_msg = f"Biller '{payee_id}' updated successfully."
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=success_msg)
            return {"status": "SUCCESS", "message": success_msg, "updated_fields": list(updates.keys())}
        else:
            err_msg = f"Biller with ID '{payee_id}' not found for user '{user_id}', or no changes were made."
            log_bq_interaction(func_name, params, query_str, status="BILLER_NOT_FOUND_OR_NO_CHANGE", error_message=err_msg)
            return {"status": "BILLER_NOT_FOUND_OR_NO_CHANGE", "message": err_msg}
    except Exception as e:
        logger.error("Exception during biller update: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_UPDATE_FAILED", error_message=str(e))
        return {"status": "ERROR_UPDATE_FAILED", "message": "Failed to update biller details."}


def remove_biller(user_id: str, payee_id: str) -> dict:
    """Removes a biller from the user's registered list."""
    func_name = "remove_biller"
    params = {"user_id": user_id, "payee_id": payee_id}
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    billers_table = _table_ref("RegisteredBillers")
    query_str = f"DELETE FROM {billers_table} WHERE user_id = @user_id AND biller_id = @payee_id"
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("payee_id", "STRING", payee_id),
        ]
    )

    try:
        query_job = client.query(query_str, job_config=job_config)
        query_job.result() # Wait for completion

        if query_job.num_dml_affected_rows > 0:
            success_msg = f"Biller with ID '{payee_id}' has been successfully removed."
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=success_msg)
            return {"status": "SUCCESS", "message": success_msg}
        else:
            err_msg = f"No biller with ID '{payee_id}' found for user '{user_id}'."
            log_bq_interaction(func_name, params, query_str, status="BILLER_NOT_FOUND", error_message=err_msg)
            return {"status": "BILLER_NOT_FOUND", "message": err_msg}
    except Exception as e:
        logger.error("Exception during biller removal: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_DELETE_FAILED", error_message=str(e))
        return {"status": "ERROR_DELETE_FAILED", "message": "Failed to remove biller."}


def list_registered_billers(user_id: str) -> dict:
    """
    Lists all registered billers for a given user.
    """
    func_name = "list_registered_billers"
    params = {"user_id": user_id}
    query_str = None
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    billers_table = _table_ref("RegisteredBillers")
    query_str = f"""
        SELECT biller_id, biller_name, bill_type, payee_nickname, last_due_amount, last_due_date
        FROM {billers_table}
        WHERE user_id = @user_id
        ORDER BY biller_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )

    try:
        query_job = client.query(query_str, job_config=job_config)
        results = list(query_job.result())

        if not results:
            log_bq_interaction(func_name, params, query_str, status="NO_BILLERS_FOUND", result_summary="No registered billers found for the user.")
            return {"status": "NO_BILLERS_FOUND", "message": "You have no registered billers."}

        billers_list = [{
            "biller_id": row.biller_id,
            "biller_name": row.biller_name,
            "bill_type": row.bill_type,
            "payee_nickname": row.payee_nickname,
            "due_amount": float(row.last_due_amount) if row.last_due_amount is not None else None,
            "due_date": row.last_due_date.isoformat() if row.last_due_date else None,
        } for row in results]

        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Found {len(billers_list)} biller(s).")
        return {"status": "SUCCESS", "billers": billers_list}
    except Exception as e:
        logger.error("Exception in list_registered_billers: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return {"status": "ERROR_QUERY_FAILED", "message": "An error occurred while fetching your billers."}


def get_accounts_for_user(user_id: str) -> list:
    """
    Retrieves all accounts (ID, type, balance) for a given user.
    """
    func_name = "get_accounts_for_user"
    params = {"user_id": user_id}
    query_str = None
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", error_message="BigQuery client not available.")
        return [{"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}]

    accounts_table = _table_ref("Accounts")
    query_str = f"""
        SELECT account_id, account_type, balance, currency
        FROM {accounts_table}
        WHERE user_id = @user_id
        ORDER BY account_type
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )

    try:
        query_job = client.query(query_str, job_config=job_config)
        results = list(query_job.result())

        if not results:
            log_bq_interaction(func_name, params, query_str, status="NO_ACCOUNTS_FOUND", result_summary="No accounts found for the user.")
            return [{"status": "NO_ACCOUNTS_FOUND", "message": "You have no accounts."}]

        accounts_list = [{
            "account_id": row.account_id,
            "account_type": row.account_type,
            "balance": float(row.balance),
            "currency": row.currency
        } for row in results]

        log_bq_interaction(func_name, params, query_str, status="SUCCESS", result_summary=f"Found {len(accounts_list)} account(s).")
        return accounts_list
    except Exception as e:
        logger.error("Exception in get_accounts_for_user: %s", str(e), exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", error_message=str(e))
        return [{"status": "ERROR_QUERY_FAILED", "message": "An error occurred while fetching your accounts."}]


def find_account_by_natural_language(user_id: str, natural_language_string: str) -> dict:
    """
    Finds the best matching account for a user based on a natural language string.
    It scores based on account_type, account_id, and synonyms.
    """
    func_name = "find_account_by_natural_language"
    params = {"user_id": user_id, "natural_language_string": natural_language_string}
    
    if not natural_language_string:
        log_bq_interaction(func_name, params, status="ERROR_INVALID_INPUT", error_message="Natural language string cannot be empty.")
        return {"status": "ERROR_INVALID_INPUT", "message": "No account specified."}

    all_accounts = get_accounts_for_user(user_id)
    if not all_accounts or "status" in all_accounts[0]: # Check if get_accounts_for_user returned an error
        log_bq_interaction(func_name, params, status="ERROR_FETCHING_ACCOUNTS", error_message="Could not fetch user accounts.")
        return {"status": "ERROR_FETCHING_ACCOUNTS", "message": "Could not retrieve your accounts to perform search."}

    search_term = natural_language_string.lower().strip()
    
    # Remove common conversational words
    conversational_words = ["my", "account", "acc"]
    for word in conversational_words:
        search_term = search_term.replace(word, "").strip()

    # Define synonyms for account types
    synonyms = {
        "checking": ["checking", "checkings", "chk", "primary"],
        "savings": ["savings", "saving", "sav"],
        "credit card": ["credit card", "credit", "cc", "visa", "mastercard"],
    }

    best_match = None
    highest_score = 0

    for account in all_accounts:
        current_score = 0
        account_type_lower = account.get("account_type", "").lower()
        account_id_lower = account.get("account_id", "").lower()

        # Score based on direct match with account_type
        if search_term == account_type_lower:
            current_score += 10

        # Score based on partial match with account_type
        if search_term in account_type_lower:
            current_score += 5

        # Score based on synonyms
        if account_type_lower in synonyms:
            if search_term in synonyms[account_type_lower]:
                current_score += 8 # High score for synonym match

        # Score based on match with account_id
        if search_term == account_id_lower:
            current_score += 10
        
        if search_term in account_id_lower:
            current_score += 2

        # Check for "account" and boost score
        if "account" in search_term and account_type_lower in search_term:
            current_score += 3

        if current_score > highest_score:
            highest_score = current_score
            best_match = account
    
    if best_match and highest_score > 3: # Threshold to avoid weak matches
        result = {
            "status": "SUCCESS",
            "account_id": best_match["account_id"],
            "account_type": best_match["account_type"],
            "balance": best_match["balance"],
            "currency": best_match["currency"],
            "search_score": highest_score
        }
        log_bq_interaction(func_name, params, status="SUCCESS", result_summary=f"Found best match: {best_match['account_id']} with score {highest_score}")
        return result
    else:
        err_msg = f"Could not find a matching account for '{natural_language_string}'. Please be more specific (e.g., 'checking account', 'savings')."
        log_bq_interaction(func_name, params, status="ERROR_ACCOUNT_NOT_FOUND", error_message=err_msg)
        return {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": err_msg}