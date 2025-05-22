# backend/bigquery_functions.py
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter
from google.api_core.exceptions import GoogleAPICallError, BadRequest
from google.oauth2 import service_account
from datetime import datetime, date

# Set up logging
logger = logging.getLogger(__name__)

# --- Configuration ---
# Ensure GOOGLE_APPLICATION_CREDENTIALS is set in your environment,
# or uncomment and configure direct credential loading if needed.
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Example custom env var
PROJECT_ID = "account-pocs" # Explicitly set project ID if not default
LOCATION = "global" # Location for Discovery Engine API
# if SERVICE_ACCOUNT_FILE:
#     creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
#     client = bigquery.Client(credentials=creds, project=PROJECT_ID if PROJECT_ID else None)
# else:
#     client = bigquery.Client(project=PROJECT_ID if PROJECT_ID else None)

try:
    # Assumes ADC or default credentials are set up
    # client = bigquery.Client(project="account-pocs", credentials=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
    client = bigquery.Client()
except Exception as e:
    print(f"Error initializing BigQuery client: {e}")
    print("Ensure GOOGLE_APPLICATION_CREDENTIALS is set or you are in a GCP environment with appropriate permissions.")
    client = None


DATASET_ID = "hdfc_voice_assistant" # UPDATED
USER_ID = "Mohit" # Hardcoded for example purposes, as in original

GLOBAL_LOG_STORE = []
# --- Helper Functions ---

def log_bq_interaction(function_name, parameters, query_str=None, status=None, result_summary=None, error_message=None):
    """
    Helper function to log BigQuery interactions for debugging and monitoring.
    
    Args:
        function_name: Name of the function making the query
        parameters: Parameters used in the query
        query_str: The SQL query string (optional)
        status: Status of the operation (SUCCESS, ERROR_*, etc.)
        result_summary: Summary of the result (optional)
        error_message: Error message if applicable (optional)
    """
    log_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_type": "BIGQUERY_INTERACTION",
        "function": function_name,
        "parameters": parameters
    }
    
    if query_str is not None:
        log_payload["query"] = query_str
    if status is not None:
        log_payload["status"] = status
    if result_summary is not None:
        log_payload["result_summary"] = result_summary
    if error_message is not None:
        log_payload["error_message"] = error_message
        
    logger.info(f"[{function_name}] {status or 'QUERY'}: {result_summary or error_message or 'Executing query'}")
    GLOBAL_LOG_STORE.append(log_payload)
def _table_ref(table_name: str) -> bigquery.TableReference | None:
    """Creates a BigQuery table reference."""
    if not client:
        print("BigQuery client not initialized. Cannot create table reference.")
        return None
    return client.dataset(DATASET_ID).table(table_name)

def _execute_query(query: str, params: list = None) -> bigquery.table.RowIterator | None:
    """Executes a DQL query and returns results."""
    if not client:
        print("BigQuery client not initialized. Cannot execute query.")
        return None
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        query_job = client.query(query, job_config=job_config)
        return query_job.result()
    except Exception as e:
        print(f"Error executing query: {e}")
        print(f"Query: {query}")
        if params:
            print(f"Params: {[p.name + ': ' + str(p.value) for p in params]}")
        return None

def _execute_dml(query: str, params: list = None) -> bool:
    """Executes a DML query and returns success status."""
    if not client:
        print("BigQuery client not initialized. Cannot execute DML.")
        return False
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        query_job = client.query(query, job_config=job_config)
        query_job.result() # Wait for DML to complete
        if query_job.errors:
            print(f"DML query failed with errors: {query_job.errors}")
            return False
        return True
    except Exception as e:
        print(f"Error executing DML: {e}")
        print(f"Query: {query}")
        if params:
            print(f"Params: {[p.name + ': ' + str(p.value) for p in params]}")
        return False

# --- Account Functions ---
def get_accounts_for_user(user_id: str) -> list:
    """
    Retrieves all accounts for a given user.

    Args:
        user_id (str): The ID of the user.

    Returns:
        list: A list of dictionaries, where each dictionary represents an account
              with keys: "account_id", "account_type", "balance", "account_nickname".
    """
    table = _table_ref('Accounts')
    if not table: return []
    query = f"""
        SELECT account_id, account_type, balance, account_nickname
        FROM `{table}`
        WHERE user_id = @user_id
    """
    params = [bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    results = _execute_query(query, params)
    accounts = []
    if results:
        for row in results:
            accounts.append({
                "account_id": row.account_id,
                "account_type": row.account_type,
                "balance": row.balance,
                "account_nickname": row.account_nickname
            })
    return accounts

def find_account_by_natural_language(user_id: str, account_name_or_type: str) -> dict | None:
    """
    Finds a specific account for a user based on a natural language query
    matching account type or nickname, with synonym mapping for account types.

    Args:
        user_id (str): The user's ID.
        account_name_or_type (str): A natural language phrase that might contain
                                    the account nickname or type.

    Returns:
        dict: The account dictionary if a match is found, otherwise None.
              Account dictionary keys: "account_id", "account_type", "balance", "account_nickname".
    """
    accounts = get_accounts_for_user(user_id)
    if not account_name_or_type:
        return None

    normalized_query = account_name_or_type.lower()

    ACCOUNT_TYPE_SYNONYMS = {
        "checking": "current",
        "checking account": "current",
        "current account": "current",
        "savings": "savings",
        "savings account": "savings",
    }

    # 1. Exact match on nickname
    for account in accounts:
        if account.get("account_nickname") and \
           normalized_query == account["account_nickname"].lower():
            return account

    # 2. Synonym-based exact account type match
    canonical_type_from_synonym = ACCOUNT_TYPE_SYNONYMS.get(normalized_query)
    if canonical_type_from_synonym:
        for account in accounts:
            if account.get("account_type") and \
               account["account_type"].lower() == canonical_type_from_synonym:
                return account

    # 3. Partial original query match against account type
    for account in accounts:
        if account.get("account_type") and \
           normalized_query in account["account_type"].lower():
            return account

    # 4. Partial original query match against account nickname
    for account in accounts:
        if account.get("account_nickname") and \
           normalized_query in account["account_nickname"].lower():
            return account
            
    return None


def _get_account_by_type(account_type: str, user_id: str) -> dict:
    """
    Helper function to get account details by account type.
    
    Args:
        account_type: The account type to look up (e.g., 'Current', 'Savings')
        user_id: The ID of the user who owns the account
        
    Returns:
        dict: Account details if found, error dict otherwise
    """
    func_name = "_get_account_by_type"
    params = {"account_type": account_type, "user_id": user_id}
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", 
                         error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    accounts_table = _table_ref("Accounts")
    
    # Normalize account type for case-insensitive comparison
    normalized_account_type = account_type.lower().capitalize()
    
    query_str = f"""
        SELECT account_id, balance, currency, account_type, account_nickname
        FROM {accounts_table}
        WHERE LOWER(account_type) = LOWER(@account_type) AND user_id = @user_id
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("account_type", "STRING", normalized_account_type),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )
    
    try:
        query_job = client.query(query_str, job_config=job_config)
        results = query_job.result()
        
        row_data = None
        for row in results:
            row_data = {
                "status": "SUCCESS",
                "account_id": row.account_id,
                "balance": float(row.balance),
                "currency": row.currency,
                "account_type": row.account_type,
                "account_name": row.account_nickname or f"{row.account_type} Account"
            }
            break
            
        if row_data:
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", 
                             result_summary=f"Account found: {row_data['account_id']} for type {account_type}")
            return row_data
        else:
            error_msg = f"Account type '{account_type}' not found for user '{user_id}'"
            log_bq_interaction(func_name, params, query_str, status="ERROR_ACCOUNT_NOT_FOUND", 
                             error_message=error_msg)
            return {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": error_msg}
            
    except Exception as e:
        error_msg = f"Error getting account by type: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", 
                         error_message=error_msg)
        return {"status": "ERROR_QUERY_FAILED", "message": error_msg}


def _get_account_balance_by_id(account_id: str, user_id: str) -> dict:
    """
    Helper function to get account balance and currency by account ID.
    
    Args:
        account_id: The account ID to look up
        user_id: The ID of the user who owns the account
        
    Returns:
        dict: Account details if found, error dict otherwise
    """
    func_name = "_get_account_balance_by_id"
    params = {"account_id": account_id, "user_id": user_id}
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", 
                         error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", "message": "BigQuery client not available."}

    accounts_table = _table_ref("Accounts")
    query_str = f"""
        SELECT account_id, balance, currency, account_type, account_nickname
        FROM {accounts_table}
        WHERE account_id = @account_id AND user_id = @user_id
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )
    
    try:
        query_job = client.query(query_str, job_config=job_config)
        results = query_job.result()
        
        row_data = None
        for row in results:
            row_data = {
                "status": "SUCCESS",
                "account_id": row.account_id,
                "balance": float(row.balance),
                "currency": row.currency,
                "account_type": row.account_type,
                "account_name": row.account_nickname or f"{row.account_type} Account"
            }
            break
            
        if row_data:
            log_bq_interaction(func_name, params, query_str, status="SUCCESS", 
                             result_summary=f"Account found: {row_data['account_id']}")
            return row_data
        else:
            error_msg = f"Account ID '{account_id}' not found for user '{user_id}'"
            log_bq_interaction(func_name, params, query_str, status="ERROR_ACCOUNT_NOT_FOUND", 
                             error_message=error_msg)
            return {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": error_msg}
            
    except Exception as e:
        error_msg = f"Error getting account balance: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        log_bq_interaction(func_name, params, query_str, status="ERROR_QUERY_FAILED", 
                         error_message=error_msg)
        return {"status": "ERROR_QUERY_FAILED", "message": error_msg}


def initiate_fund_transfer_check(from_account_type: str, to_account_type: str, amount: float) -> dict:
    """
    Checks if a fund transfer is possible between two account types for the USER_ID.
    
    Args:
        from_account_type: The type of account to transfer from (e.g., 'Current', 'Savings')
        to_account_type: The type of account to transfer to (e.g., 'Current', 'Savings')
        amount: The amount to transfer (must be positive)
        
    Returns:
        dict: Status and details of the transfer check
    """
    func_name = "initiate_fund_transfer_check"
    params = {"from_account_type": from_account_type, 
             "to_account_type": to_account_type, 
             "amount": amount,
             "user_id": USER_ID}
    
    if not client:
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", 
                "message": "BigQuery client not available."}
    
    if not isinstance(amount, (int, float)) or amount <= 0:
        return {"status": "ERROR_INVALID_AMOUNT", 
                "message": "Transfer amount must be a positive number."}
    
    try:
        # Get source account details by account type
        from_account = _get_account_by_type(from_account_type, USER_ID)
        if from_account.get("status") != "SUCCESS":
            return from_account
            
        # Get destination account details by account type
        to_account = _get_account_by_type(to_account_type, USER_ID)
        if to_account.get("status") != "SUCCESS":
            return to_account
            
        # Check if accounts are the same
        if from_account["account_id"] == to_account["account_id"]:
            return {"status": "ERROR_SAME_ACCOUNT", 
                    "message": "Cannot transfer to the same account."}
        
        # Check if source has sufficient balance
        if from_account["balance"] < amount:
            return {
                "status": "INSUFFICIENT_FUNDS",
                "message": f"Insufficient funds. Available: {from_account['balance']} {from_account['currency']}",
                "current_balance": from_account["balance"],
                "requested_amount": amount,
                "currency": from_account["currency"],
                "from_account_id": from_account["account_id"],
                "to_account_id": to_account["account_id"]
            }
            
        # All checks passed
        return {
            "status": "SUFFICIENT_FUNDS",
            "from_account_id": from_account["account_id"],
            "from_account_name": from_account.get("account_name", from_account_type),
            "to_account_id": to_account["account_id"],
            "to_account_name": to_account.get("account_name", to_account_type),
            "currency": from_account["currency"],
            "amount": amount,
            "message": "Transfer check successful. Ready to execute."
        }
        
    except Exception as e:
        error_msg = f"Error during transfer check: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        return {"status": "ERROR_EXCEPTION", 
                "message": f"An error occurred during transfer check: {str(e)}"}


def execute_fund_transfer(from_account_id: str, to_account_id: str, amount: float, currency: str, memo: str) -> dict:
    """
    Executes a fund transfer between two accounts as an atomic operation.
    
    Args:
        from_account_id: The account ID to transfer from
        to_account_id: The account ID to transfer to
        amount: The amount to transfer (must be positive)
        currency: The currency of the transfer
        memo: A memo/description for the transaction
        
    Returns:
        dict: Status and details of the transfer
    """
    func_name = "execute_fund_transfer"
    params = {
        "from_account_id": from_account_id,
        "to_account_id": to_account_id,
        "amount": amount,
        "currency": currency,
        "memo": memo,
        "user_id": USER_ID
    }
    
    if not client:
        log_bq_interaction(func_name, params, status="ERROR_CLIENT_NOT_INITIALIZED", 
                         error_message="BigQuery client not available.")
        return {"status": "ERROR_CLIENT_NOT_INITIALIZED", 
                "message": "BigQuery client not available."}
    
    if not isinstance(amount, (int, float)) or amount <= 0:
        return {"status": "ERROR_INVALID_AMOUNT", 
                "message": "Transfer amount must be a positive number."}
    
    if from_account_id == to_account_id:
        return {"status": "ERROR_SAME_ACCOUNT", 
                "message": "Cannot transfer to the same account."}
    
    # Fetch account details for validation
    from_account = _get_account_balance_by_id(from_account_id, USER_ID)
    if from_account.get("status") != "SUCCESS":
        return from_account
        
    to_account = _get_account_balance_by_id(to_account_id, USER_ID)
    if to_account.get("status") != "SUCCESS":
        return to_account
    
    # Validate currencies match
    if from_account["currency"] != currency or to_account["currency"] != currency:
        return {
            "status": "ERROR_CURRENCY_MISMATCH",
            "message": f"Currency mismatch. From: {from_account['currency']}, To: {to_account['currency']}, Requested: {currency}"
        }
    
    # Check sufficient balance
    if from_account["balance"] < amount:
        return {
            "status": "INSUFFICIENT_FUNDS",
            "message": f"Insufficient funds. Available: {from_account['balance']} {currency}",
            "current_balance": from_account["balance"],
            "requested_amount": amount,
            "currency": currency,
            "from_account_id": from_account_id,
            "to_account_id": to_account_id
        }
    
    # Generate transaction IDs and timestamp
    transaction_base_id = f"txn_{uuid.uuid4().hex}"
    debit_transaction_id = f"{transaction_base_id}_D"
    credit_transaction_id = f"{transaction_base_id}_C"
    current_timestamp = datetime.now(timezone.utc).isoformat()
    
    # Get table references
    accounts_table = _table_ref("Accounts")
    transactions_table = _table_ref("Transactions")
    
    # Prepare the multi-statement transaction
    query_str = f"""
    BEGIN TRANSACTION;

    -- 1. Decrement sender's balance
    UPDATE {accounts_table}
    SET balance = balance - @amount
    WHERE account_id = @from_account_id AND user_id = @user_id;

    -- 2. Increment recipient's balance
    UPDATE {accounts_table}
    SET balance = balance + @amount
    WHERE account_id = @to_account_id AND user_id = @user_id;

    -- 3. Insert debit transaction for sender
    INSERT INTO {transactions_table} (
        transaction_id, account_id, user_id, date, 
        description, amount, currency, type, memo
    ) VALUES (
        @debit_transaction_id, @from_account_id, @user_id, @timestamp,
        @debit_description, -@amount, @currency, 'transfer_debit', @memo
    );

    -- 4. Insert credit transaction for recipient
    INSERT INTO {transactions_table} (
        transaction_id, account_id, user_id, date, 
        description, amount, currency, type, memo
    ) VALUES (
        @credit_transaction_id, @to_account_id, @user_id, @timestamp,
        @credit_description, @amount, @currency, 'transfer_credit', @memo
    );

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
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", current_timestamp),
            bigquery.ScalarQueryParameter("debit_description", "STRING", 
                f"Transfer to account {to_account.get('account_name', to_account_id)}"),
            bigquery.ScalarQueryParameter("credit_description", "STRING", 
                f"Transfer from account {from_account.get('account_name', from_account_id)}"),
            bigquery.ScalarQueryParameter("currency", "STRING", currency),
            bigquery.ScalarQueryParameter("memo", "STRING", memo or "")
        ]
    )
    
    try:
        logger.info(f"[{func_name}] Executing fund transfer transaction for user {USER_ID} "
                  f"from {from_account_id} to {to_account_id} for {amount} {currency}.")
        query_job = client.query(query_str, job_config=job_config)
        query_job.result()  # Wait for the transaction to complete

        if query_job.errors:
            error_detail = f"BigQuery transaction failed: {query_job.errors}"
            log_bq_interaction(func_name, params, query_str, 
                             status="ERROR_TRANSACTION_FAILED", 
                             error_message=error_detail)
            return {
                "status": "ERROR_TRANSACTION_FAILED", 
                "message": "Fund transfer failed during BigQuery execution.", 
                "details": query_job.errors
            }

        success_msg = (f"Fund transfer of {amount} {currency} "
                      f"from {from_account_id} to {to_account_id} completed successfully. "
                      f"Transaction ID: {transaction_base_id}")
        
        log_bq_interaction(func_name, params, query_str, 
                         status="SUCCESS", 
                         result_summary=success_msg)
        
        return {
            "status": "SUCCESS",
            "transaction_id": transaction_base_id,
            "message": success_msg,
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "amount": amount,
            "currency": currency,
            "timestamp": current_timestamp
        }
        
    except GoogleAPICallError as e:
        error_msg = f"Google API error during fund transfer: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        log_bq_interaction(func_name, params, query_str, 
                         status="ERROR_GOOGLE_API", 
                         error_message=error_msg)
        return {
            "status": "ERROR_GOOGLE_API",
            "message": "A Google Cloud service error occurred during the transfer.",
            "details": str(e)
        }
    except BadRequest as e:
        error_msg = f"Invalid request during fund transfer: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        log_bq_interaction(func_name, params, query_str, 
                         status="ERROR_INVALID_REQUEST", 
                         error_message=error_msg)
        return {
            "status": "ERROR_INVALID_REQUEST",
            "message": "The fund transfer request was invalid.",
            "details": str(e)
        }
    except Exception as e:
        error_msg = f"Unexpected error during fund transfer: {str(e)}"
        logger.error(f"[{func_name}] {error_msg}", exc_info=True)
        log_bq_interaction(func_name, params, query_str, 
                         status="ERROR_UNKNOWN", 
                         error_message=error_msg)
        return {
            "status": "ERROR_UNKNOWN",
            "message": "An unexpected error occurred during the fund transfer.",
            "details": str(e)
        }

# --- Transaction Functions ---
def get_transactions_for_account(user_id: str, account_id: str, limit: int = 10) -> list:
    """
    Retrieves transactions for a specific account of a user.

    Args:
        user_id (str): The ID of the user.
        account_id (str): The ID of the account.
        limit (int): Maximum number of transactions to retrieve.

    Returns:
        list: A list of dictionaries, where each dictionary represents a transaction
              with keys: "transaction_id", "date", "description", "amount", "type", "category".
    """
    table = _table_ref('Transactions')
    if not table: return []
    query = f"""
        SELECT transaction_id, date, description, amount, type
        FROM `{table}`
        WHERE user_id = @user_id AND account_id = @account_id
        ORDER BY date DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
        bigquery.ScalarQueryParameter("limit", "INT64", limit)
    ]
    results = _execute_query(query, params)
    transactions = []
    if results:
        for row in results:
            transactions.append({
                "transaction_id": row.transaction_id,
                "date": row.date.strftime("%Y-%m-%d %H:%M:%S") if row.date else None,
                "description": row.description,
                "amount": row.amount,
                "type": row.type
            })
    return transactions

# --- Biller Functions ---
def list_registered_billers(user_id: str) -> list:
    """
    Lists all registered billers for a user.

    Args:
        user_id (str): The ID of the user.

    Returns:
        list: A list of dictionaries, where each dictionary represents a registered biller
              with keys: "biller_id", "biller_nickname", "biller_name", "account_number_at_biller",
              "last_due_amount", "last_due_date", "bill_type".
    """
    table = _table_ref('RegisteredBillers')
    if not table: return []
    query = f"""
        SELECT
            biller_id,
            biller_name,
            biller_nickname,
            account_number_at_biller,
            last_due_amount,
            last_due_date,
            bill_type
        FROM `{table}`
        WHERE user_id = @user_id
        ORDER BY biller_nickname
    """
    params = [bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    results = _execute_query(query, params)
    billers = []
    if results:
        for row in results:
            # Sanitize biller_id in the output
            sanitized_biller_id = sanitize_biller_id(row.biller_id)
            
            # Use the official biller_name from the database
            biller_name = row.biller_name
            if not biller_name:
                # Fallback to nickname or bill type if official name is not available
                biller_name = row.biller_nickname or f"{row.bill_type.title()} Bill"
                
            billers.append({
                "biller_id": sanitized_biller_id,
                "biller_nickname": row.biller_nickname,
                "biller_name": biller_name,
                "account_number_at_biller": row.account_number_at_biller,
                "last_due_amount": row.last_due_amount,
                "last_due_date": str(row.last_due_date) if row.last_due_date else None,
                "bill_type": row.bill_type
            })
    return billers

def find_biller_by_nickname(user_id: str, biller_nickname: str) -> dict | None:
    """
    Finds a biller based on user ID and biller nickname (case-insensitive).

    Args:
        user_id (str): The ID of the user.
        biller_nickname (str): The nickname of the biller to find.

    Returns:
        dict: A dictionary containing the biller's details (including biller_id)
              if a match is found, otherwise None.
    """
    table = _table_ref('RegisteredBillers')
    if not table: return None
    query = f"""
        SELECT
            biller_id,
            biller_nickname,
            account_number_at_biller,
            last_due_amount,
            last_due_date,
            bill_type
        FROM `{table}`
        WHERE user_id = @user_id AND LOWER(biller_nickname) = LOWER(@biller_nickname)
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("biller_nickname", "STRING", biller_nickname)
    ]
    results = _execute_query(query, params)
    if results:
        for row in results: # Should be only one or none
            return {
                "biller_id": row.biller_id,
                "biller_nickname": row.biller_nickname,
                "account_number_at_biller": row.account_number_at_biller,
                "last_due_amount": row.last_due_amount,
                "last_due_date": str(row.last_due_date) if row.last_due_date else None,
                "bill_type": row.bill_type
            }
    return None

def get_bill_details(user_id: str, biller_id: str) -> dict | None:
    """
    Retrieves details for a specific biller.

    Args:
        user_id (str): The ID of the user.
        biller_id (str): The ID of the biller.

    Returns:
        dict: A dictionary containing bill details with keys: "biller_nickname",
              "account_number_at_biller", "last_due_amount", "last_due_date", "bill_type",
              or None if not found.
    """
    table = _table_ref('RegisteredBillers')
    if not table: return None
    query = f"""
        SELECT
            biller_nickname,
            account_number_at_biller,
            last_due_amount,
            last_due_date,
            bill_type
        FROM `{table}`
        WHERE user_id = @user_id AND biller_id = @biller_id
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("biller_id", "STRING", biller_id)
    ]
    results = _execute_query(query, params)
    if results:
        for row in results: # Should be only one or none
            return {
                "biller_nickname": row.biller_nickname,
                "account_number_at_biller": row.account_number_at_biller,
                "last_due_amount": row.last_due_amount,
                "last_due_date": str(row.last_due_date) if row.last_due_date else None,
                "bill_type": row.bill_type
            }
    return None

def register_biller(user_id: str, biller_id: str, biller_nickname: str,
                    account_number_at_biller: str, last_due_amount: float | None,
                    last_due_date: str | date | None, bill_type: str) -> bool:
    """
    Registers a new biller for the user.

    Args:
        user_id (str): User's ID.
        biller_id (str): Unique ID for the biller registration.
        biller_nickname (str): User-defined nickname for the biller.
        account_number_at_biller (str): User's account number with the biller.
        last_due_amount (float | None): Current due amount. Can be None.
        last_due_date (str | date | None): Due date (e.g., 'YYYY-MM-DD' string or date object). Can be None.
        bill_type (str): Type of bill (e.g., 'ELECTRICITY', 'WATER').

    Returns:
        bool: True if registration was successful, False otherwise.
    """
    table = _table_ref('RegisteredBillers')
    if not table: return False
    
    # Sanitize the biller_id before insertion
    sanitized_biller_id = sanitize_biller_id(biller_id)
    
    query = f"""
        INSERT INTO `{table}` (
            user_id, biller_id, biller_nickname, account_number_at_biller,
            last_due_amount, last_due_date, bill_type
        )
        VALUES (
            @user_id, @biller_id, @biller_nickname, @account_number_at_biller,
            @last_due_amount, @last_due_date, @bill_type
        )
    """
    parsed_due_date = None
    if isinstance(last_due_date, str):
        try:
            parsed_due_date = datetime.strptime(last_due_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid date format for last_due_date: {last_due_date}. Expected YYYY-MM-DD. Storing as NULL.")
    elif isinstance(last_due_date, date):
        parsed_due_date = last_due_date

    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("biller_id", "STRING", sanitized_biller_id),
        bigquery.ScalarQueryParameter("biller_nickname", "STRING", biller_nickname),
        bigquery.ScalarQueryParameter("account_number_at_biller", "STRING", account_number_at_biller),
        bigquery.ScalarQueryParameter("last_due_amount", "FLOAT64", last_due_amount),
        bigquery.ScalarQueryParameter("last_due_date", "DATE", parsed_due_date),
        bigquery.ScalarQueryParameter("bill_type", "STRING", bill_type)
    ]
    return _execute_dml(query, params)

def update_biller_details(user_id: str, biller_id: str,
                          new_biller_nickname: str | None = None,
                          new_account_number_at_biller: str | None = None) -> bool:
    """
    Updates details for an existing registered biller.
    Only updates biller_nickname and account_number_at_biller.

    Args:
        user_id (str): User's ID.
        biller_id (str): Biller's ID.
        new_biller_nickname (str, optional): New nickname for the biller.
        new_account_number_at_biller (str, optional): New account number with the biller.

    Returns:
        bool: True if update was successful, False otherwise.
    """
    table = _table_ref('RegisteredBillers')
    if not table: return False
    set_clauses = []
    params_for_set = []

    if new_biller_nickname is not None:
        set_clauses.append("biller_nickname = @new_biller_nickname")
        params_for_set.append(bigquery.ScalarQueryParameter("new_biller_nickname", "STRING", new_biller_nickname))
    if new_account_number_at_biller is not None:
        set_clauses.append("account_number_at_biller = @new_account_number_at_biller")
        params_for_set.append(bigquery.ScalarQueryParameter("new_account_number_at_biller", "STRING", new_account_number_at_biller))

    if not set_clauses:
        print("No details provided for update.")
        return False # Or True, if no change means success

    query = f"""
        UPDATE `{table}`
        SET {', '.join(set_clauses)}
        WHERE user_id = @user_id AND biller_id = @biller_id
    """
    base_params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("biller_id", "STRING", biller_id)
    ]
    return _execute_dml(query, base_params + params_for_set)


def remove_biller(user_id: str, biller_id: str) -> bool:
    """
    Removes a registered biller for the user by deleting the record.

    Args:
        user_id (str): User's ID.
        biller_id (str): Biller's ID to remove.

    Returns:
        bool: True if removal (deletion) was successful, False otherwise.
    """
    table = _table_ref('RegisteredBillers')
    if not table: return False
    query = f"""
        DELETE FROM `{table}`
        WHERE user_id = @user_id AND biller_id = @biller_id
    """
    params = [
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("biller_id", "STRING", biller_id)
    ]
    return _execute_dml(query, params)

def sanitize_biller_id(biller_id: str) -> str:
    """
    Sanitizes the biller ID to remove problematic characters.
    
    Args:
        biller_id (str): The biller ID to sanitize.
        
    Returns:
        str: The sanitized biller ID.
    """
    # Remove slashes and other potentially problematic characters
    sanitized = biller_id.replace("/", "_").replace("\\", "_")
    # Replace any other problematic characters as needed
    return sanitized

def pay_bill(user_id: str, biller_nickname: str, payment_amount: float, timestamp: datetime = None, from_account_id: str = None) -> dict:
    """
    Processes a bill payment by:
    1. Verifying the biller exists
    2. Checking account balance
    3. Deducting the payment amount from the specified account (or default account)
    4. Recording the transaction
    5. Updating the biller's last payment information

    Args:
        user_id (str): User's ID.
        biller_nickname (str): Biller's nickname (case-insensitive).
        payment_amount (float): The amount being paid.
        timestamp (datetime, optional): The timestamp of the payment. Defaults to current UTC time.
        from_account_id (str, optional): Specific account ID to use for payment. If not provided,
                                      will use biller's default account or find a suitable one.

    Returns:
        dict: A dictionary with payment status and details.
              On success: {'status': 'success', 'biller_name': str, 'amount_paid': float, 
                          'due_date': str, 'account_id': str, 'transaction_id': str}
              On failure: {'status': 'error', 'message': str}
    """
    table = _table_ref('RegisteredBillers')
    if not table: 
        return {"status": "error", "message": "Database connection error"}

    # Clean and normalize the nickname
    biller_nickname = biller_nickname.strip() if biller_nickname else ""
    if not biller_nickname:
        return {"status": "error", "message": "Biller nickname cannot be empty"}
    if payment_amount <= 0:
        return {"status": "error", "message": "Payment amount must be greater than zero"}

    # Find the biller by nickname (case-insensitive)
    biller_details = find_biller_by_nickname(user_id, biller_nickname)
    if not biller_details:
        return {"status": "error", "message": f"No biller found with nickname: {biller_nickname}"}
    
    # Get the biller's official name for the response
    biller_name = biller_details.get("biller_name") or biller_details.get("biller_nickname") or "Unknown Biller"
    
    # Set payment date (default to now if not provided)
    payment_date = timestamp.date() if timestamp else datetime.utcnow().date()
    payment_timestamp = timestamp or datetime.utcnow()

    # Begin transaction
    try:
        # 1. Get the payment account
        account_id = from_account_id or biller_details.get("default_payment_account_id")
        if not account_id:
            # If no specific account provided and no default account, find a suitable account
            accounts = get_accounts_for_user(user_id)
            if not accounts:
                return {"status": "error", "message": "No payment accounts found"}
                
            # Try to find a current account with sufficient balance first
            current_accounts = [acc for acc in accounts 
                              if acc.get("account_type", "").lower() == "current" 
                              and acc.get("balance", 0) >= payment_amount]
            
            if current_accounts:
                account_id = current_accounts[0]["account_id"]
            else:
                # Find any account with sufficient balance
                for acc in accounts:
                    if acc.get("balance", 0) >= payment_amount:
                        account_id = acc["account_id"]
                        break
                
                if not account_id:
                    return {"status": "error", "message": "No account with sufficient balance found"}
        
        # 2. Verify account exists and has sufficient balance
        account_table = _table_ref('Accounts')
        if not account_table:
            return {"status": "error", "message": "Database error: Could not access accounts"}
            
        # 3. Deduct amount from account balance using atomic update
        update_account_query = f"""
            UPDATE `{account_table}`
            SET balance = balance - @payment_amount
            WHERE user_id = @user_id 
              AND account_id = @account_id
              AND balance >= @payment_amount
        """
        account_params = [
            bigquery.ScalarQueryParameter("payment_amount", "FLOAT64", payment_amount),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id)
        ]
        
        # Execute the account update
        rows_updated = _execute_dml(update_account_query, account_params)
        if not rows_updated:
            return {"status": "error", "message": "Insufficient balance or account not found"}
        
        # 4. Record the transaction
        transaction_id = f"txn_{user_id}_{int(payment_timestamp.timestamp())}"
        transaction_table = _table_ref('Transactions')
        if not transaction_table:
            return {"status": "error", "message": "Database error: Could not access transactions"}
            
        insert_transaction_query = f"""
            INSERT INTO `{transaction_table}`
            (transaction_id, user_id, account_id, date, description, amount, currency, type, memo)
            VALUES (@transaction_id, @user_id, @account_id, @date, @description, @amount, @currency, @type, @memo)
        """
        transaction_params = [
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("date", "TIMESTAMP", payment_timestamp),
            bigquery.ScalarQueryParameter("description", "STRING", f"Bill payment to {biller_name}"),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", -payment_amount),  # Negative for debit
            bigquery.ScalarQueryParameter("currency", "STRING", "INR"),  # Assuming INR, can be parameterized if needed
            bigquery.ScalarQueryParameter("type", "STRING", "debit"),
            bigquery.ScalarQueryParameter("memo", "STRING", f"Bill payment for {biller_name}")
        ]
        
        if not _execute_dml(insert_transaction_query, transaction_params):
            return {"status": "error", "message": "Failed to record transaction"}
        
        # 5. Update the biller's record
        update_biller_query = f"""
            UPDATE `{table}`
            SET last_due_amount = 0.0, 
                last_due_date = @payment_date
            WHERE user_id = @user_id 
              AND biller_id = @biller_id
        """
        biller_params = [
            bigquery.ScalarQueryParameter("payment_date", "DATE", payment_date),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("biller_id", "STRING", biller_details["biller_id"])
        ]
        
        if not _execute_dml(update_biller_query, biller_params):
            return {"status": "error", "message": "Failed to update biller record"}
        
        # If we get here, all operations were successful
        return {
            "status": "success",
            "biller_name": biller_name,
            "account_id": account_id,
            "amount_paid": payment_amount,
            "due_date": payment_date.isoformat(),
            "transaction_id": transaction_id,
            "message": f"Successfully paid {payment_amount:.2f} to {biller_name} from account {account_id}"
        }
        
    except Exception as e:
        print(f"Error in pay_bill: {str(e)}")
        return {
            "status": "error",
            "message": f"An error occurred while processing the payment: {str(e)}"
        }

# --- Example Usage (for testing, can be removed or commented out) ---
if __name__ == '__main__':
    if not client:
        print("BigQuery client not available. Exiting example usage.")
        exit()

    print(f"Using DATASET_ID: {DATASET_ID}")
    current_user_id = USER_ID # Using the hardcoded USER_ID for tests
    print(f"Using USER_ID for tests: {current_user_id}\n")

    print("--- Accounts ---")
    accounts = get_accounts_for_user(current_user_id)
    if accounts:
        for acc_idx, acc in enumerate(accounts):
            print(f"  Account ID: {acc['account_id']}, Type: {acc['account_type']}, "
                  f"Nickname: {acc.get('account_nickname', 'N/A')}, Balance: {acc['balance']}")
            # Test transactions for the first account found
            if acc_idx == 0:
                print(f"    Transactions for Account ID {acc['account_id']}:")
                transactions = get_transactions_for_account(current_user_id, acc['account_id'], limit=3)
                if transactions:
                    for t in transactions:
                        print(f"      {t['date']} - {t['description']}: {t['amount']} ({t['type']})")
                else:
                    print(f"      No transactions found for account {acc['account_id']} or error.")
    else:
        print(f"  No accounts found for user {current_user_id} or error.")

    print("\n--- Find Account (Natural Language) ---")
    # Test with a type that might exist (e.g., 'Savings' if in sample data)
    found_acc_type = find_account_by_natural_language(current_user_id, "Savings")
    if found_acc_type:
        print(f"  Found account (by type 'Savings'): ID {found_acc_type['account_id']} - Nickname: {found_acc_type.get('account_nickname', 'N/A')}, Type: {found_acc_type['account_type']}")
    else:
        print("  Account matching type 'Savings' not found.")

    # Test with a nickname that might exist (e.g., 'My Main Savings' if in sample data)
    found_acc_nick = find_account_by_natural_language(current_user_id, "My Main Savings")
    if found_acc_nick:
        print(f"  Found account (by nickname 'My Main Savings'): ID {found_acc_nick['account_id']} - Nickname: {found_acc_nick.get('account_nickname', 'N/A')}, Type: {found_acc_nick['account_type']}")
    else:
        print("  Account matching nickname 'My Main Savings' not found.")

    # Test with synonym "checking"
    found_acc_checking = find_account_by_natural_language(current_user_id, "checking")
    if found_acc_checking:
        print(f"  Found account (by synonym 'checking' -> 'current'): ID {found_acc_checking['account_id']} - Nickname: {found_acc_checking.get('account_nickname', 'N/A')}, Type: {found_acc_checking['account_type']}")
    else:
        print("  Account matching synonym 'checking' not found.")

    # Test with synonym "checking account"
    found_acc_checking_account = find_account_by_natural_language(current_user_id, "checking account")
    if found_acc_checking_account:
        print(f"  Found account (by synonym 'checking account' -> 'current'): ID {found_acc_checking_account['account_id']} - Nickname: {found_acc_checking_account.get('account_nickname', 'N/A')}, Type: {found_acc_checking_account['account_type']}")
    else:
        print("  Account matching synonym 'checking account' not found.")


    print("\n--- Billers ---")
    biller_id_to_test = "test_biller_py_001" # Example biller ID, unique for test isolation
    biller_nickname_test = "My Test Biller"
    biller_acc_num_test = "TB12345"
    biller_due_amt_test = 50.25
    biller_due_date_test = "2025-08-15"
    biller_type_test = "INTERNET"

    # Clean up if exists from previous run
    print(f"Attempting to remove biller {biller_id_to_test} if it exists (cleanup)...")
    remove_biller(current_user_id, biller_id_to_test)

    print(f"Attempting to register biller: {biller_id_to_test} ({biller_nickname_test})")
    reg_success = register_biller(
        user_id=current_user_id,
        biller_id=biller_id_to_test,
        biller_nickname=biller_nickname_test,
        account_number_at_biller=biller_acc_num_test,
        last_due_amount=biller_due_amt_test,
        last_due_date=biller_due_date_test,
        bill_type=biller_type_test
    )
    print(f"  Registration successful: {reg_success}")

    if reg_success:
        print(f"\nDetails for biller {biller_id_to_test}:")
        details = get_bill_details(current_user_id, biller_id_to_test)
        if details:
            print(f"  {details}")
        else:
            print(f"  Could not retrieve details for {biller_id_to_test}")

        print(f"\nAttempting to update biller {biller_id_to_test} nickname...")
        update_success = update_biller_details(
            user_id=current_user_id,
            biller_id=biller_id_to_test,
            new_biller_nickname="My Updated Test Biller"
        )
        print(f"  Update successful: {update_success}")
        if update_success:
            details_after_update = get_bill_details(current_user_id, biller_id_to_test)
            if details_after_update:
                print(f"  Updated details: {details_after_update}")
            else:
                print(f"  Could not retrieve details for {biller_id_to_test} after update.")


        print(f"\nAttempting to pay bill for {biller_nickname_test}...")
        pay_success = pay_bill(current_user_id, biller_nickname_test, biller_due_amt_test)
        print(f"  Payment successful: {pay_success}")
        if pay_success:
            details_after_payment = get_bill_details(current_user_id, biller_id_to_test)
            if details_after_payment:
                print(f"  Details after payment: {details_after_payment}") # last_due_amount should be 0.0
            else:
                print(f"  Could not retrieve details for {biller_id_to_test} after payment.")


        print(f"\nAttempting to remove biller {biller_id_to_test}...")
        remove_success = remove_biller(current_user_id, biller_id_to_test)
        print(f"  Removal successful: {remove_success}")
        if remove_success:
            details_after_removal = get_bill_details(current_user_id, biller_id_to_test)
            if not details_after_removal:
                print(f"  Biller {biller_id_to_test} successfully removed (details are None).")
            else:
                print(f"  Biller {biller_id_to_test} still exists after removal attempt.")
    else:
        print(f"Skipping further biller tests as registration for {biller_id_to_test} failed.")

    # Example of trying to find an account that doesn't exist
    print("\n--- Find Non-existent Account ---")
    non_existent_account = find_account_by_natural_language(current_user_id, "My Imaginary Account")
    if non_existent_account:
        print(f"  Error: Found account for 'My Imaginary Account': {non_existent_account}")
    else:
        print("  Correctly did not find 'My Imaginary Account'.")

    print("\nExample usage finished.")