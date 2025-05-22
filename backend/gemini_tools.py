from google.genai import types
from google.cloud import discoveryengine
from google.api_core.client_options import ClientOptions
import asyncio # For potential future use with to_thread
import bigquery_functions
from bigquery_functions import USER_ID # Import USER_ID
import json
import logging
import time
import uuid # Added for generating biller_id
from datetime import datetime, timezone, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Helper function for structured logging
YOUR_DATASTORE_ID = "bank-faq-demo-ds_1747707296437"
SEARCH_ENGINE_ID = "bank-faq-demo_1747707209997"
def _log_tool_event(event_type: str, tool_name: str, parameters: dict, response: dict = None, status: str = None, result: dict = None, error_message: str = None):
    """Helper function to create and print a structured log entry for tool events."""
    log_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_type": "TOOL_EVENT",
        "event_subtype": event_type,
        "tool_function_name": tool_name,
        "parameters_sent": parameters
    }
    if response is not None:
        log_payload["response_received"] = response
    if status is not None:
        log_payload["status"] = status
    if result is not None:
        log_payload["result"] = result
    if error_message is not None:
        log_payload["error_message"] = error_message
    print(json.dumps(log_payload))

# --- Tool Declarations and Implementations ---

# 1. getBalance
getBalance_declaration = types.FunctionDeclaration(
    name="getBalance",
    description="Fetches details for a specified bank account, including balance. Uses natural language to find the account (e.g., 'current', 'my savings account').",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "account_type": types.Schema(type=types.Type.STRING, description="The type or nickname of the account to fetch details for (e.g., 'current', 'savings', 'Mohit Primary Current').")
        },
        required=["account_type"]
    )
)

async def getBalance(account_type: str):
    tool_name = "getBalance"
    params_sent = {"account_type": account_type}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    logger.info(f"[{tool_name}] Attempting to find account by natural language: {account_type}")
    api_response = {}
    try:
        # bigquery_functions.find_account_by_natural_language returns account details dict or None
        account_details = bigquery_functions.find_account_by_natural_language(USER_ID, account_type)
        logger.info(f"[{tool_name}] Received from bigquery_functions.find_account_by_natural_language: {account_details}")

        if account_details:
            api_response = {
                "status": "success",
                "account_id": account_details.get("account_id"),
                "account_type": account_details.get("account_type"),
                "account_nickname": account_details.get("account_nickname"),
                "balance": account_details.get("balance"),
                "currency": account_details.get("currency") # Assuming currency is part of account_details
            }
        else:
            api_response = {"status": "error", "message": f"Account '{account_type}' not found or error fetching details."}
    except Exception as e:
        logger.error(f"[{tool_name}] Error calling BQ or processing result: {e}", exc_info=True)
        api_response = {"status": "error", "message": f"An internal error occurred while fetching account balance for {account_type}."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 2. getTransactionHistory
getTransactionHistory_declaration = types.FunctionDeclaration(
    name="getTransactionHistory",
    description="Fetches the last N transactions for a specified bank account. Uses natural language to find the account first.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "account_type": types.Schema(type=types.Type.STRING, description="The type or nickname of the account (e.g., 'current', 'savings', 'Mohit Primary Current')."),
            "limit": types.Schema(type=types.Type.INTEGER, description="The number of transactions to retrieve (defaults to 10).")
        },
        required=["account_type"]
    )
)

async def getTransactionHistory(account_type: str, limit: int = 10): # Default limit matches BQ function
    tool_name = "getTransactionHistory"
    params_sent = {"account_type": account_type, "limit": limit}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    logger.info(f"[{tool_name}] Finding account '{account_type}' to get transaction history.")
    api_response = {}
    try:
        account_details = bigquery_functions.find_account_by_natural_language(USER_ID, account_type)
        if not account_details or not account_details.get("account_id"):
            logger.warning(f"[{tool_name}] Account '{account_type}' not found.")
            api_response = {"status": "error", "message": f"Account '{account_type}' not found."}
        else:
            account_id = account_details["account_id"]
            logger.info(f"[{tool_name}] Account ID '{account_id}' found for '{account_type}'. Fetching transactions with limit: {limit}")
            # bigquery_functions.get_transactions_for_account returns a list of transaction dicts
            bq_transactions = bigquery_functions.get_transactions_for_account(USER_ID, account_id, limit)
            logger.info(f"[{tool_name}] Received {len(bq_transactions)} transactions from BQ for account_id {account_id}")

            formatted_transactions = []
            for t in bq_transactions:
                # BQ returns: "transaction_id", "date", "description", "amount", "type", "category"
                # SQL schema for Transactions has "currency", but BQ function does not select it.
                formatted_transactions.append({
                    "id": t.get("transaction_id"), # Map to "id" for tool response
                    "date": t.get("date"),
                    "description": t.get("description"),
                    "amount": t.get("amount"),
                    "transaction_type": t.get("type"), # "type" from BQ maps to "transaction_type"
                    "category": t.get("category")
                    # "currency" is NOT returned by bigquery_functions.get_transactions_for_account
                })
            api_response = {
                "status": "success",
                "account_id": account_id,
                "account_type": account_details.get("account_type"),
                "transactions": formatted_transactions
            }
    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": f"An internal error occurred while fetching transaction history for {account_type}."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 3. initiateFundTransfer
initiateFundTransfer_declaration = types.FunctionDeclaration(
    name="initiateFundTransfer",
    description="Checks feasibility and prepares for a fund transfer between two accounts. Resolves accounts using natural language, checks balance, and confirms currency.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "amount": types.Schema(type=types.Type.NUMBER, description="The amount to transfer."),
            "currency": types.Schema(type=types.Type.STRING, description="The currency of the amount (e.g., 'INR'). This will be validated against the source account's currency."),
            "from_account_type": types.Schema(type=types.Type.STRING, description="The type or nickname of the account to transfer from (e.g., 'current', 'Mohit Primary Current')."),
            "to_account_type": types.Schema(type=types.Type.STRING, description="The type or nickname of the account to transfer to (e.g., 'savings', 'Mohit Savings Fund').")
        },
        required=["amount", "currency", "from_account_type", "to_account_type"]
    )
)

async def initiateFundTransfer(amount: float, currency: str, from_account_type: str, to_account_type: str):
    """
    Initiates a fund transfer between two accounts after performing necessary validations.
    
    This function checks if the transfer is possible by verifying account balances,
    account existence, and other constraints before allowing the transfer to proceed.
    
    Args:
        amount: The amount to transfer (must be positive)
        currency: The currency of the transfer (e.g., 'USD', 'INR')
        from_account_type: Type of the source account (e.g., 'checking', 'savings')
        to_account_type: Type of the destination account (e.g., 'checking', 'savings')
        
    Returns:
        dict: Status and details of the transfer initiation
    """
    tool_name = "initiateFundTransfer"
    params = {
        "amount": amount,
        "currency": currency,
        "from_account_type": from_account_type,
        "to_account_type": to_account_type,
        "user_id": USER_ID
    }
    
    try:
        # Log the initiation attempt
        _log_tool_event(
            "INITIATE_TRANSFER", 
            tool_name, 
            params,
            status="STARTED"
        )
        
        logger.info(f"[{tool_name}] Initiating transfer: {amount} {currency} from '{from_account_type}' to '{to_account_type}'.")

        # Use the initiate_fund_transfer_check function for validation
        transfer_check = bigquery_functions.initiate_fund_transfer_check(
            from_account_type=from_account_type,
            to_account_type=to_account_type,
            amount=amount
        )
        
        # Log the transfer check result
        _log_tool_event(
            "TRANSFER_CHECK",
            tool_name,
            params,
            status=transfer_check.get("status", "UNKNOWN"),
            result=transfer_check
        )
        
        if transfer_check.get("status") == "SUFFICIENT_FUNDS":
            # Return confirmation details without executing the transfer
            api_response = {
                "status": "requires_confirmation",
                "message": f"Please confirm transfer of {amount} {currency} from {from_account_type} (ID: {transfer_check.get('from_account_id')}) to {to_account_type} (ID: {transfer_check.get('to_account_id')}).",
                "transfer_details": {
                    "amount": amount,
                    "currency": currency,
                    "from_account_type": from_account_type,
                    "from_account_id": transfer_check.get("from_account_id"),
                    "to_account_type": to_account_type,
                    "to_account_id": transfer_check.get("to_account_id"),
                    "confirmation_id": f"confirm_{transfer_check.get('from_account_id')}_{transfer_check.get('to_account_id')}_{amount}"
                }
            }
            
            _log_tool_event(
                "CONFIRMATION_REQUIRED",
                tool_name,
                params,
                status="SUCCESS",
                result=api_response
            )
            
            return api_response
        else:
            # Return the error from the transfer check
            api_response = {
                "status": "error",
                "message": transfer_check.get("message", "Fund transfer initiation failed.")
            }
            
            _log_tool_event(
                "TRANSFER_CHECK_FAILED",
                tool_name,
                params,
                status=transfer_check.get("status", "ERROR"),
                error_message=transfer_check.get("message", "Unknown error")
            )
            
            return api_response
            
    except Exception as e:
        error_msg = f"Error initiating fund transfer: {str(e)}"
        logger.error(f"[{tool_name}] {error_msg}", exc_info=True)
        
        _log_tool_event(
            "ERROR",
            tool_name,
            params,
            status="ERROR_EXCEPTION",
            error_message=error_msg
        )
        
        return {
            "status": "error",
            "message": "An internal error occurred while initiating fund transfer."
        }

# 4. executeFundTransfer
executeFundTransfer_declaration = types.FunctionDeclaration(
    name="executeFundTransfer",
    description="Executes a fund transfer between two accounts after validation.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "amount": types.Schema(type=types.Type.NUMBER, description="The amount to transfer."),
            "currency": types.Schema(type=types.Type.STRING, description="The currency of the amount."),
            "from_account_id": types.Schema(type=types.Type.STRING, description="The ID of the source account."),
            "to_account_id": types.Schema(type=types.Type.STRING, description="The ID of the destination account."),
            "memo": types.Schema(type=types.Type.STRING, description="Optional memo/description for the transaction.")
        },
        required=["amount", "currency", "from_account_id", "to_account_id"]
    )
)

async def executeFundTransfer(amount: float, currency: str, from_account_id: str, to_account_id: str, memo: str = None):
    """
    Executes a fund transfer between two accounts after confirmation.
    
    This function performs the actual transfer of funds between accounts after
    the user has confirmed the transaction details.
    
    Args:
        amount: The amount to transfer (must be positive)
        currency: The currency of the transfer (e.g., 'USD', 'INR')
        from_account_id: The ID of the source account
        to_account_id: The ID of the destination account
        memo: Optional memo/description for the transaction
        
    Returns:
        dict: Status and details of the executed transfer
    """
    tool_name = "executeFundTransfer"
    params_sent = {
        "amount": amount,
        "currency": currency,
        "from_account_id": from_account_id,
        "to_account_id": to_account_id,
        "memo": memo
    }
    
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    logger.info(f"[{tool_name}] Attempting to call bigquery_functions.execute_fund_transfer with from_account_id: {from_account_id}, to_account_id: {to_account_id}, amount: {amount}, currency: {currency}, memo: {memo}")
    
    api_response = {}
    try:
        # The BQ function `execute_fund_transfer` simulates the transfer and logs.
        transfer_result = bigquery_functions.execute_fund_transfer(
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            amount=amount,
            currency=currency,
            memo=memo or f"Transfer from {from_account_id} to {to_account_id}"
        )
        
        logger.info(f"[{tool_name}] Received from bigquery_functions.execute_fund_transfer: {transfer_result}")
        
        if transfer_result.get("status") == "SUCCESS":
            api_response = {
                "status": "success",
                "message": "Transfer completed successfully.",
                "details": {
                    "transaction_id": transfer_result.get("transaction_id"),
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                    "amount": amount,
                    "currency": currency,
                    "timestamp": transfer_result.get("timestamp", datetime.now(timezone.utc).isoformat())
                }
            }
        else:
            api_response = {
                "status": "error",
                "message": transfer_result.get("message", "Fund transfer execution failed.")
            }
    except Exception as e:
        logger.error(f"[{tool_name}] Error calling BQ or processing result for executeFundTransfer: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while executing fund transfer."}
    
    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# Helper: resolve_biller_by_name (adapted for direct BQ call)
async def resolve_biller_by_name(user_id: str, biller_name_query: str) -> dict:
    tool_name = "_resolve_biller_by_name_helper" # Internal helper
    params_sent = {"user_id": user_id, "biller_name_query": biller_name_query}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    logger.info(f"[{tool_name}] Attempting to find biller by name/nickname '{biller_name_query}' for user {user_id}")

    try:
        # bigquery_functions.list_registered_billers returns a direct list of biller dicts
        all_billers = bigquery_functions.list_registered_billers(user_id)
        logger.info(f"[{tool_name}] Retrieved {len(all_billers)} billers for user {user_id}")

        if not all_billers:
            response = {"status": "ERROR_BILLER_NOT_FOUND", "message": f"No billers registered for user {user_id}."}
            _log_tool_event("INVOCATION_END", tool_name, params_sent, response)
            return response

        normalized_query = biller_name_query.lower()
        exact_matches = []
        partial_matches = []

        for biller in all_billers:
            # BQ returns: biller_id, biller_nickname, account_number_at_biller, last_due_amount, last_due_date, bill_type
            # SQL schema has biller_name, but BQ list_registered_billers does not select it. Match on biller_nickname.
            biller_nickname = biller.get("biller_nickname", "").lower()

            if biller_nickname == normalized_query:
                exact_matches.append(biller)
            elif normalized_query in biller_nickname:
                partial_matches.append(biller)
        
        final_match = None
        if exact_matches:
            if len(exact_matches) == 1:
                final_match = exact_matches[0]
            else: # Multiple exact matches
                options = [{"biller_id": b["biller_id"], "biller_nickname": b.get("biller_nickname")} for b in exact_matches]
                response = {"status": "ERROR_AMBIGUOUS_BILLER", "message": f"Multiple billers found matching '{biller_name_query}'. Please be more specific.", "options": options}
                _log_tool_event("INVOCATION_END", tool_name, params_sent, response)
                return response
        elif partial_matches:
            if len(partial_matches) == 1:
                final_match = partial_matches[0]
            else: # Multiple partial matches
                options = [{"biller_id": b["biller_id"], "biller_nickname": b.get("biller_nickname")} for b in partial_matches]
                response = {"status": "ERROR_AMBIGUOUS_BILLER", "message": f"Multiple billers partially match '{biller_name_query}'. Please be more specific.", "options": options}
                _log_tool_event("INVOCATION_END", tool_name, params_sent, response)
                return response

        if final_match:
            response = {
                "status": "success",
                "biller_id": final_match["biller_id"],
                "biller_nickname": final_match.get("biller_nickname"), # This is the field we matched on
                "account_number_at_biller": final_match.get("account_number_at_biller"),
                "bill_type": final_match.get("bill_type")
            }
        else:
            response = {"status": "ERROR_BILLER_NOT_FOUND", "message": f"Biller '{biller_name_query}' not found."}

        _log_tool_event("INVOCATION_END", tool_name, params_sent, response)
        return response

    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        error_response = {"status": "error", "message": f"Internal error resolving biller: {str(e)}"}
        _log_tool_event("INVOCATION_END", tool_name, params_sent, error_response)
        return error_response

# 5. getBillDetails
getBillDetails_declaration = types.FunctionDeclaration(
    name="getBillDetails",
    description="Fetches details for a specific biller. You can provide a biller nickname (e.g., 'My Electric Co.') or a bill type (e.g., 'electricity'). If bill type is used and multiple billers match, clarification may be needed.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "payee_nickname": types.Schema(type=types.Type.STRING, description="Optional. The nickname of the biller (e.g., 'My Electric Co.'). This corresponds to 'biller_nickname' in the database."),
            "bill_type": types.Schema(type=types.Type.STRING, description="Optional. The type of bill (e.g., 'electricity', 'water').")
        },
        required=[] # At least one should be provided by the LLM based on context.
    )
)

async def getBillDetails(payee_nickname: str = None, bill_type: str = None):
    tool_name = "getBillDetails"
    params_sent = {"payee_nickname": payee_nickname, "bill_type": bill_type}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}

    if not payee_nickname and not bill_type:
        api_response = {"status": "error", "message": "Please provide either a payee nickname or a bill type."}
        _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
        return api_response

    try:
        biller_id_to_query = None
        resolved_biller_nickname = None

        if payee_nickname:
            logger.info(f"[{tool_name}] Resolving biller by nickname: {payee_nickname}")
            resolution_result = await resolve_biller_by_name(USER_ID, payee_nickname)
            if resolution_result.get("status") == "success":
                biller_id_to_query = resolution_result["biller_id"]
                resolved_biller_nickname = resolution_result.get("biller_nickname")
            else:
                api_response = resolution_result # Pass error/ambiguity from helper
                _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
                return api_response
        
        elif bill_type: # No nickname, try by bill_type
            logger.info(f"[{tool_name}] Listing billers to filter by type: {bill_type}")
            all_user_billers = bigquery_functions.list_registered_billers(USER_ID)
            matching_billers = [b for b in all_user_billers if b.get("bill_type", "").lower() == bill_type.lower()]

            if not matching_billers:
                api_response = {"status": "error", "message": f"No billers found for type '{bill_type}'."}
                _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
                return api_response
            if len(matching_billers) > 1:
                options = [{"biller_id": b["biller_id"], "biller_nickname": b.get("biller_nickname")} for b in matching_billers]
                api_response = {"status": "clarification_needed", "message": f"Multiple billers found for type '{bill_type}'. Please specify.", "options": options}
                _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
                return api_response
            biller_id_to_query = matching_billers[0]["biller_id"]
            resolved_biller_nickname = matching_billers[0].get("biller_nickname")

        if biller_id_to_query:
            logger.info(f"[{tool_name}] Fetching details for biller_id: {biller_id_to_query}")
            # bigquery_functions.get_bill_details returns dict or None
            # Keys: "biller_nickname", "account_number_at_biller", "last_due_amount", "last_due_date", "bill_type"
            bq_result = bigquery_functions.get_bill_details(USER_ID, biller_id_to_query)
            logger.info(f"[{tool_name}] Received from BQ get_bill_details: {bq_result}")

            if bq_result:
                api_response = {
                    "status": "success",
                    "payee_id": biller_id_to_query, # This is the resolved/found biller_id
                    "payee_nickname": bq_result.get("biller_nickname"), # From BQ direct call
                    "account_number_at_biller": bq_result.get("account_number_at_biller"),
                    "due_amount": bq_result.get("last_due_amount"),
                    "due_date": bq_result.get("last_due_date"),
                    "bill_type": bq_result.get("bill_type")
                }
            else:
                api_response = {"status": "error", "message": f"Could not retrieve details for biller ID '{biller_id_to_query}'."}
        else: # Should not happen if logic above is correct, but as a fallback
            api_response = {"status": "error", "message": "Could not determine a biller to query."}

    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while fetching bill details."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# Helper: resolve_account_by_name (for payBill)
async def resolve_account_by_name(user_id: str, natural_language_name: str) -> dict:
    tool_name = "_resolve_account_by_name_helper"
    params_sent = {"user_id": user_id, "natural_language_name": natural_language_name}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    logger.info(f"[{tool_name}] Attempting to call bigquery_functions.find_account_by_natural_language for user {user_id} with name '{natural_language_name}'")
    
    try:
        account_details = bigquery_functions.find_account_by_natural_language(user_id, natural_language_name)
        logger.info(f"[{tool_name}] Received from bigquery_functions.find_account_by_natural_language: {account_details}")
        
        if account_details:
            response = {"status": "success", "account": account_details}
        else:
            response = {"status": "ERROR_ACCOUNT_NOT_FOUND", "message": f"Account '{natural_language_name}' not found."}
        
        _log_tool_event("INVOCATION_END", tool_name, params_sent, response)
        return response
        
    except Exception as e:
        logger.error(f"[{tool_name}] Error calling BQ or processing result: {e}", exc_info=True)
        error_response = {"status": "error", "message": f"An internal error occurred while resolving account name: {str(e)}"}
        _log_tool_event("INVOCATION_END", tool_name, params_sent, error_response)
        return error_response

# 6. payBill
payBill_declaration = types.FunctionDeclaration(
    name="payBill",
    description="Updates the status of a bill in the biller list (e.g., sets due amount to zero and updates due date). It does NOT perform financial deduction from an account. The 'from_account_id' parameter is for logging/intent but not used for deduction by the current backend.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "payee_id": types.Schema(type=types.Type.STRING, description="The nickname or unique ID of the biller to pay. If a nickname is provided, it will be resolved to an ID."),
            "amount": types.Schema(type=types.Type.NUMBER, description="The amount that was notionally paid. The backend will set the due amount to 0."),
            "from_account_id": types.Schema(type=types.Type.STRING, description="Optional. The ID or natural language description (e.g., 'my savings') of the account notionally used for payment. This is for record-keeping and not used for actual deduction by the backend.")
        },
        required=["payee_id", "amount"]
    )
)

async def payBill(payee_id: str, amount: float, from_account_id: str = None): # from_account_id is optional
    tool_name = "payBill"
    params_sent = {"payee_id": payee_id, "amount": amount, "from_account_id": from_account_id}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}
    
    try:
        logger.info(f"[{tool_name}] Attempting to pay bill for payee/biller: '{payee_id}', amount: {amount}.")
        if from_account_id: # Try to resolve if it's a name
            logger.info(f"[{tool_name}] 'from_account_id' provided as '{from_account_id}'. This is for logging purposes only.")
        
        # First, check if the payee_id is a valid biller ID
        biller_details = bigquery_functions.get_bill_details(USER_ID, payee_id)
        
        if biller_details:
            # payee_id is a valid biller ID
            biller_id = payee_id
            biller_nickname = biller_details.get("biller_nickname", payee_id)
            logger.info(f"[{tool_name}] Using provided biller ID '{biller_id}' with nickname '{biller_nickname}'.")
        else:
            # Try to resolve payee_id as a nickname
            logger.info(f"[{tool_name}] Payee '{payee_id}' not found as biller ID, trying to resolve as nickname...")
            resolution_result = await resolve_biller_by_name(USER_ID, payee_id)
            
            if resolution_result.get("status") != "success":
                api_response = resolution_result # Pass error/ambiguity
                _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
                return api_response
            
            biller_id = resolution_result["biller_id"]
            biller_nickname = resolution_result.get("biller_nickname", payee_id)
            logger.info(f"[{tool_name}] Resolved payee '{payee_id}' to biller_id '{biller_id}' with nickname '{biller_nickname}'.")

        payment_timestamp = datetime.now(timezone.utc)
        
        # bigquery_functions.pay_bill updates RegisteredBillers, sets due to 0, updates due date.
        # It takes user_id, biller_nickname, payment_amount (note: BQ uses this for logging, sets due to 0), timestamp.
        success = bigquery_functions.pay_bill(USER_ID, biller_nickname, amount, payment_timestamp)
        
        if success:
            api_response = {
                "status": "success",
                "message": f"Bill for '{biller_nickname}' (ID: {biller_id}) has been marked as paid. Due amount set to 0.",
                "biller_id": biller_id,
                "biller_nickname": biller_nickname,
                "amount_paid": amount,
                "payment_timestamp": payment_timestamp.isoformat()
            }
        else:
            api_response = {
                "status": "error", 
                "message": f"Failed to mark bill as paid for biller '{biller_nickname}' (ID: {biller_id})."
            }

    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {
            "status": "error", 
            "message": f"An internal error occurred while processing payment: {str(e)}"
        }

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 7. registerBiller
registerBiller_declaration = types.FunctionDeclaration(
    name="registerBiller",
    description="Registers a new biller for the user. A unique 'biller_id' will be generated. 'biller_name' and 'default_payment_account_id' are NOT used by the current backend function but can be provided for future use.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "biller_name": types.Schema(type=types.Type.STRING, description="The official name of the biller company. (Currently NOT stored by backend function)."),
            "biller_type": types.Schema(type=types.Type.STRING, description="The category of the bill (e.g., 'electricity', 'internet'). Corresponds to 'bill_type' in the database."),
            "account_number": types.Schema(type=types.Type.STRING, description="The user's account number with the biller. Corresponds to 'account_number_at_biller' in the database."),
            "payee_nickname": types.Schema(type=types.Type.STRING, description="Optional. A nickname for this biller. Corresponds to 'biller_nickname' in the database."),
            "default_payment_account_id": types.Schema(type=types.Type.STRING, description="Optional. The ID of the user's bank account for default payments. (Currently NOT stored by backend function)."),
            "due_amount": types.Schema(type=types.Type.NUMBER, description="Optional. The current due amount. Corresponds to 'last_due_amount' in the database."),
            "due_date": types.Schema(type=types.Type.STRING, description="Optional. The current due date in YYYY-MM-DD format. Corresponds to 'last_due_date' in the database.")
        },
        # biller_id is generated by the tool, not taken as input from LLM.
        # BQ function requires: user_id, biller_id, biller_nickname, account_number_at_biller, last_due_amount, last_due_date, bill_type
        required=["biller_type", "account_number"] # Core info needed for BQ
    )
)

async def registerBiller(biller_type: str, account_number: str, biller_name: str = None, payee_nickname: str = None, default_payment_account_id: str = None, due_amount: float = None, due_date: str = None):
    tool_name = "registerBiller"
    # Log all params received by the tool
    params_sent = {
        "biller_name": biller_name, "biller_type": biller_type, "account_number": account_number,
        "payee_nickname": payee_nickname, "default_payment_account_id": default_payment_account_id,
        "due_amount": due_amount, "due_date": due_date
    }
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}
    try:
        # Generate a unique biller_id for the new registration
        generated_biller_id = f"biller_{USER_ID.lower()}_{uuid.uuid4().hex[:8]}"
        logger.info(f"[{tool_name}] Registering new biller. Generated biller_id: {generated_biller_id}")
        logger.info(f"[{tool_name}] Biller name (tool input, not used by BQ): {biller_name}")
        logger.info(f"[{tool_name}] Default payment account ID (tool input, not used by BQ): {default_payment_account_id}")

        # Parameters for bigquery_functions.register_biller:
        # user_id, biller_id, biller_nickname, account_number_at_biller, last_due_amount, last_due_date, bill_type
        success = bigquery_functions.register_biller(
            user_id=USER_ID,
            biller_id=generated_biller_id,
            biller_nickname=payee_nickname, # Use payee_nickname from tool for biller_nickname in BQ
            account_number_at_biller=account_number, # account_number from tool
            last_due_amount=due_amount, # due_amount from tool
            last_due_date=due_date, # due_date from tool (string YYYY-MM-DD or date obj)
            bill_type=biller_type # biller_type from tool
        )

        if success:
            api_response = {
                "status": "success",
                "message": f"Biller '{payee_nickname or biller_name or 'Unnamed Biller'}' registered successfully with ID '{generated_biller_id}'.",
                "biller_id": generated_biller_id
            }
        else:
            api_response = {"status": "error", "message": "Failed to register biller."}

    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while registering biller."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 8. updateBillerDetails
updateBillerDetails_declaration = types.FunctionDeclaration(
    name="updateBillerDetails",
    description="Updates details for an existing registered biller. Currently, only 'payee_nickname' (biller_nickname) and 'account_number' (account_number_at_biller) can be updated.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "payee_id": types.Schema(type=types.Type.STRING, description="The unique ID of the biller to update. This corresponds to 'biller_id' in the database."),
            "updates": types.Schema(
                type=types.Type.OBJECT,
                description="A dictionary of fields to update. Only 'payee_nickname' and 'account_number' are supported.",
                properties={
                    "payee_nickname": types.Schema(type=types.Type.STRING, description="New nickname for this biller. Maps to 'biller_nickname' in DB."),
                    "account_number": types.Schema(type=types.Type.STRING, description="New user's account number with the biller. Maps to 'account_number_at_biller' in DB.")
                },
                # No required fields within updates; if a key is present, its value is used.
            )
        },
        required=["payee_id", "updates"]
    )
)

async def updateBillerDetails(payee_id: str, updates: dict):
    tool_name = "updateBillerDetails"
    params_sent = {"payee_id": payee_id, "updates": updates}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}
    try:
        logger.info(f"[{tool_name}] Updating biller ID '{payee_id}' with updates: {updates}")

        # Parameters for bigquery_functions.update_biller_details:
        # user_id, biller_id, new_biller_nickname=None, new_account_number_at_biller=None
        new_biller_nickname = updates.get("payee_nickname") # Maps to new_biller_nickname
        new_account_number = updates.get("account_number") # Maps to new_account_number_at_biller

        if not new_biller_nickname and not new_account_number:
            api_response = {"status": "error", "message": "No valid update fields provided. Only 'payee_nickname' and 'account_number' can be updated."}
            _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
            return api_response

        success = bigquery_functions.update_biller_details(
            user_id=USER_ID,
            biller_id=payee_id, # payee_id from tool is biller_id for BQ
            new_biller_nickname=new_biller_nickname,
            new_account_number_at_biller=new_account_number
        )

        if success:
            api_response = {"status": "success", "message": f"Biller '{payee_id}' updated successfully."}
        else:
            api_response = {"status": "error", "message": f"Failed to update biller '{payee_id}'."}

    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while updating biller details."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 9. removeBiller
removeBiller_declaration = types.FunctionDeclaration(
    name="removeBiller",
    description="Deletes a registered biller from the system for the user.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "payee_id": types.Schema(type=types.Type.STRING, description="The unique ID of the biller to remove (this is the 'biller_id').")
        },
        required=["payee_id"]
    )
)

async def removeBiller(payee_id: str):
    tool_name = "removeBiller"
    params_sent = {"payee_id": payee_id}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}
    try:
        logger.info(f"[{tool_name}] Removing biller ID: {payee_id}")
        # bigquery_functions.remove_biller takes user_id, biller_id
        success = bigquery_functions.remove_biller(USER_ID, payee_id) # payee_id from tool is biller_id for BQ

        if success:
            api_response = {"status": "success", "message": f"Biller '{payee_id}' removed successfully."}
        else:
            api_response = {"status": "error", "message": f"Failed to remove biller '{payee_id}'. It might not exist or an error occurred."}
    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while removing biller."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response

# 10. listRegisteredBillers
listRegisteredBillers_declaration = types.FunctionDeclaration(
    name="listRegisteredBillers",
    description="Lists all billers registered by the user. Note: 'biller_name' (official company name) is not included in the response from the current backend.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={} # No parameters
    )
)

async def listRegisteredBillers():
    tool_name = "listRegisteredBillers"
    params_sent = {}
    _log_tool_event("INVOCATION_START", tool_name, params_sent)
    api_response = {}
    try:
        logger.info(f"[{tool_name}] Listing all registered billers for user {USER_ID}")
        # bigquery_functions.list_registered_billers returns a direct list of biller dicts
        # Keys: "biller_id", "biller_nickname", "account_number_at_biller", "last_due_amount", "last_due_date", "bill_type"
        # SQL schema has "biller_name", but BQ function does not select it.
        bq_billers = bigquery_functions.list_registered_billers(USER_ID)
        logger.info(f"[{tool_name}] Received {len(bq_billers)} billers from BQ.")

        api_response = {
            "status": "success",
            "billers": bq_billers # Pass the list directly as per BQ function's output structure
        }
    except Exception as e:
        logger.error(f"[{tool_name}] Error: {e}", exc_info=True)
        api_response = {"status": "error", "message": "An internal error occurred while listing registered billers."}

    _log_tool_event("INVOCATION_END", tool_name, params_sent, api_response)
    return api_response


def search_spec():
    content_search_spec = discoveryengine.SearchRequest.ContentSearchSpec(
        snippet_spec=discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
            return_snippet=True
        ),
        summary_spec=discoveryengine.SearchRequest.ContentSearchSpec.SummarySpec(
            summary_result_count=3,
            ignore_adversarial_query=True,
        ),
    )
    return content_search_spec

async def search_faq(search_query: str) -> str:
    """Searches and provides answers to bank-related Frequently Asked Questions (FAQs).

    This function first checks for common banking queries that can be answered directly.
    For other queries, it utilizes Google Cloud Discovery Engine to find relevant information.
    """
    # Convert query to lowercase for case-insensitive matching
    query_lower = search_query.lower()
    
    # Common banking queries with direct responses
    common_queries = {
        'operating hours': 'Our standard banking hours are Monday to Friday from 9:30 AM to 3:30 PM, and select branches are open on Saturdays from 9:30 AM to 1:30 PM. Timings may vary by location.',
        'banking hours': 'Our standard banking hours are Monday to Friday from 9:30 AM to 3:30 PM. Some branches may have extended hours or be open on Saturdays.',
        'working hours': 'Our branches are typically open Monday to Friday from 9:30 AM to 3:30 PM. Digital banking services are available 24/7.',
        'when do you open': 'Most of our branches open at 9:30 AM from Monday to Friday.',
        'when do you close': 'Most of our branches close at 3:30 PM from Monday to Friday.',
        'weekend hours': 'Select branches are open on Saturdays from 9:30 AM to 1:30 PM. All branches are closed on Sundays and public holidays.',
        'holiday hours': 'Branches are closed on public holidays. Please check our website or mobile app for the holiday calendar.',
        '24 hour customer service': 'Our customer service is available 24/7 at 1800-123-1234 for any banking assistance.',
        'customer service hours': 'Our customer service is available 24/7 at 1800-123-1234.',
        'emergency contact': 'For lost or stolen cards, please call our 24/7 helpline at 1800-123-1234 immediately.'
    }
    
    # Check if the query matches any common questions
    for question, answer in common_queries.items():
        if question in query_lower:
            return answer
    
    # For other queries, use the Discovery Engine
    try:
        location = "global"
        project_id = "account-pocs"
        engine_id = SEARCH_ENGINE_ID

        client_options = (
            ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
            if location != "global"
            else None
        )

        client = discoveryengine.SearchServiceClient(client_options=client_options)
        serving_config = f"projects/{project_id}/locations/{location}/collections/default_collection/engines/{engine_id}/servingConfigs/default_config"
        content_search_spec_config = search_spec()

        request = discoveryengine.SearchRequest(
            serving_config=serving_config,
            query=search_query,
            page_size=5,  # Reduced from 10 to get more relevant results
            content_search_spec=content_search_spec_config,
            query_expansion_spec=discoveryengine.SearchRequest.QueryExpansionSpec(
                condition=discoveryengine.SearchRequest.QueryExpansionSpec.Condition.AUTO,
            ),
            spell_correction_spec=discoveryengine.SearchRequest.SpellCorrectionSpec(
                mode=discoveryengine.SearchRequest.SpellCorrectionSpec.Mode.AUTO
            ),
        )
        
        response = await asyncio.to_thread(client.search, request)
        
        # If we have a summary, return it
        if hasattr(response, 'summary') and response.summary.summary_text:
            return response.summary.summary_text
            
        # If no summary but we have search results, return the first one
        if hasattr(response, 'results') and response.results:
            return response.results[0].document.derived_struct_data.get('snippets', [{}])[0].get('snippet', 'I found some information but couldn\'t generate a proper summary.')
            
        return "I couldn't find specific information about that. Would you like me to connect you with a customer service representative?"
        
    except Exception as e:
        logger.error(f"Error in search_faq: {str(e)}")
        return "I'm having trouble accessing the FAQ system right now. Please try again later or contact customer support at 1800-123-1234."
# Function Declaration for search_faq
search_faq_declaration = types.FunctionDeclaration(
    name="search_faq",
    description="Searches and provides answers to bank-related Frequently Asked Questions (FAQs) using Google Cloud Discovery Engine.",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "search_query": types.Schema(type=types.Type.STRING, description="The search query from the user.")
        },
        required=["search_query"]
    )
)
# search_faq_declaration, # This line is removed as the declaration is added to banking_tool.function_declarations
# Tool instance containing all function declarations
banking_tool = types.Tool(
    function_declarations=[
        getBalance_declaration,
        getTransactionHistory_declaration,
        initiateFundTransfer_declaration,
        executeFundTransfer_declaration,
        getBillDetails_declaration,
        payBill_declaration,
        registerBiller_declaration,
        updateBillerDetails_declaration,
        removeBiller_declaration,
        listRegisteredBillers_declaration,
        search_faq_declaration,
    ]
)

# Example of how you might want to export or use this tool
# (This part is for demonstration and might need adjustment based on your project structure)
#
# available_tools = {
# "banking_tool": banking_tool
# }
#
# if __name__ == "__main__":
# # You can print the tool to see its structure
#     import json
#     print(json.dumps(banking_tool.to_dict(), indent=2))