-- bigquery_setup.sql
-- This script defines the schemas for Accounts, Transactions, and RegisteredBillers tables
-- and populates them with synthetic data for use in the Gemini Live Python Backend.

-- ====================================================================================
-- Table Definitions
-- ====================================================================================

-- ------------------------------------------------------------------------------------
-- Accounts Table
-- Stores user account information.
-- ------------------------------------------------------------------------------------
CREATE TABLE hdfc_voice_assistant.Accounts (
    user_id STRING NOT NULL,                            -- Identifier for the user
    account_id STRING NOT NULL,                         -- Unique identifier for the account (Primary Key)
    account_type STRING NOT NULL,                       -- Type of account (e.g., "current", "savings")
    balance FLOAT64 NOT NULL,                           -- Current balance of the account
    currency STRING NOT NULL,                           -- Currency code (e.g., "INR")
    account_nickname STRING,                            -- Optional nickname for the account
    -- PRIMARY KEY (account_id) -- Declared for clarity, BigQuery doesn't enforce traditional PKs
    -- UNIQUE (account_id) -- Declared for clarity
);

-- ------------------------------------------------------------------------------------
-- Transactions Table
-- Stores all financial transactions for user accounts.
-- ------------------------------------------------------------------------------------
CREATE TABLE hdfc_voice_assistant.Transactions (
    transaction_id STRING NOT NULL,                     -- Unique identifier for the transaction (Primary Key)
    user_id STRING NOT NULL,                            -- Identifier for the user associated with the transaction
    account_id STRING NOT NULL,                         -- Identifier for the account involved (Foreign Key to Accounts.account_id)
    date TIMESTAMP NOT NULL,                            -- Date and time of the transaction
    description STRING NOT NULL,                        -- Description of the transaction
    amount FLOAT64 NOT NULL,                            -- Transaction amount (negative for debit, positive for credit)
    currency STRING NOT NULL,                           -- Currency code (e.g., "INR")
    type STRING NOT NULL,                               -- Type of transaction (e.g., "debit", "credit", "transfer")
    memo STRING,                                        -- Optional memo, especially for fund transfers
    -- PRIMARY KEY (transaction_id) -- Declared for clarity
    -- UNIQUE (transaction_id) -- Declared for clarity
    -- FOREIGN KEY (account_id) REFERENCES Accounts(account_id) -- Declared for clarity
);

-- ------------------------------------------------------------------------------------
-- RegisteredBillers Table
-- Stores information about billers registered by users.
-- ------------------------------------------------------------------------------------
CREATE TABLE hdfc_voice_assistant.RegisteredBillers (
    biller_id STRING NOT NULL,                          -- Unique identifier for the biller registration (Primary Key)
    user_id STRING NOT NULL,                            -- Identifier for the user who registered the biller
    biller_name STRING NOT NULL,                        -- Official name of the biller (e.g., "City Power", "PG&E")
    biller_nickname STRING,                             -- Optional user-defined nickname for the biller (e.g., "My Electric Co.")
    account_number_at_biller STRING NOT NULL,           -- User's account number with the biller
    default_payment_account_id STRING,                  -- Optional default account_id for paying this biller (Foreign Key to Accounts.account_id)
    last_due_amount FLOAT64,                            -- Last known due amount for the bill
    last_due_date DATE,                                 -- Last known due date for the bill
    bill_type STRING,                                   -- Type of bill (e.g., "electricity", "water", "internet")
    -- PRIMARY KEY (biller_id) -- Declared for clarity
    -- UNIQUE (biller_id) -- Declared for clarity
    -- FOREIGN KEY (default_payment_account_id) REFERENCES Accounts(account_id) -- Declared for clarity
);

-- ====================================================================================
-- Synthetic Data Insertion
-- ====================================================================================

-- ------------------------------------------------------------------------------------
-- User 1: Mohit (user_id: Mohit)
-- ------------------------------------------------------------------------------------

-- Krishnan's Accounts
INSERT INTO hdfc_voice_assistant.Accounts (user_id, account_id, account_type, balance, currency, account_nickname) VALUES
('Mohit', 'acc_mohit_curr_001', 'Current', 1250.75, 'INR', 'Mohit Primary Current'),
('Mohit', 'acc_mohit_sav_002', 'Savings', 5300.00, 'INR', 'Mohit Savings Fund');

-- Mohit's Transactions (Current Account: acc_mohit_curr_001)
INSERT INTO hdfc_voice_assistant.Transactions (transaction_id, user_id, account_id, date, description, amount, currency, type, memo) VALUES
('txn_m_curr_001', 'Mohit', 'acc_mohit_curr_001', TIMESTAMP('2025-05-15T08:30:00Z'), 'Coffee Shop Morning Brew', -4.50, 'INR', 'debit', NULL),
('txn_m_curr_002', 'Mohit', 'acc_mohit_curr_001', TIMESTAMP('2025-05-14T17:00:00Z'), 'Salary Deposit - May', 2500.00, 'INR', 'credit', 'Monthly Salary'),
('txn_m_curr_003', 'Mohit', 'acc_mohit_curr_001', TIMESTAMP('2025-05-14T10:15:00Z'), 'Online Purchase - Tech Gadget', -199.99, 'INR', 'debit', 'New Headphones'),
('txn_m_curr_004', 'Mohit', 'acc_mohit_curr_001', TIMESTAMP('2025-05-13T12:05:00Z'), 'Grocery Store Run', -75.20, 'INR', 'debit', NULL),
('txn_m_curr_005', 'Mohit', 'acc_mohit_curr_001', TIMESTAMP('2025-05-12T09:00:00Z'), 'Transfer to Savings', -500.00, 'INR', 'transfer', 'Monthly savings transfer');

-- Mohit's Transactions (Savings Account: acc_mohit_sav_002)
INSERT INTO hdfc_voice_assistant.Transactions (transaction_id, user_id, account_id, date, description, amount, currency, type, memo) VALUES
('txn_m_sav_001', 'Mohit', 'acc_mohit_sav_002', TIMESTAMP('2025-05-12T09:00:05Z'), 'Transfer from Current', 500.00, 'INR', 'transfer', 'Monthly savings transfer'),
('txn_m_sav_002', 'Mohit', 'acc_mohit_sav_002', TIMESTAMP('2025-04-30T10:00:00Z'), 'Interest Earned April', 12.50, 'INR', 'credit', 'Monthly Interest');

-- Mohit's Registered Billers
INSERT INTO hdfc_voice_assistant.RegisteredBillers (biller_id, user_id, biller_name, biller_nickname, account_number_at_biller, default_payment_account_id, last_due_amount, last_due_date, bill_type) VALUES
('biller_m_elec_001', 'Mohit', 'Maharashtra State Electricity Distribution Company Limited', 'Maharashtra State Electricity Distribution Company Limited', 'CP-987654321', 'acc_mohit_curr_001', 75.50, DATE('2025-05-20'), 'electricity');

-- ------------------------------------------------------------------------------------
-- User 2: Priya (user_id: user_priya_002)
-- ------------------------------------------------------------------------------------

-- Alex's Accounts
INSERT INTO hdfc_voice_assistant.Accounts (user_id, account_id, account_type, balance, currency, account_nickname) VALUES
('user_priya_002', 'acc_priya_curr_001', 'Current', 800.00, 'INR', 'Priya Everyday Account');

-- Priya's Transactions (Current Account: acc_priya_curr_001)
INSERT INTO hdfc_voice_assistant.Transactions (transaction_id, user_id, account_id, date, description, amount, currency, type, memo) VALUES
('txn_p_curr_001', 'user_priya_002', 'acc_priya_curr_001', TIMESTAMP('2025-05-15T10:00:00Z'), 'Lunch with Friends', -25.00, 'INR', 'debit', NULL),
('txn_p_curr_002', 'user_priya_002', 'acc_priya_curr_001', TIMESTAMP('2025-05-13T18:30:00Z'), 'Movie Tickets', -30.00, 'INR', 'debit', 'Cinema Night'),
('txn_p_curr_003', 'user_priya_002', 'acc_priya_curr_001', TIMESTAMP('2025-05-10T11:00:00Z'), 'Bookstore Purchase', -15.75, 'INR', 'debit', NULL);

-- Priya's Registered Billers
INSERT INTO hdfc_voice_assistant.RegisteredBillers (biller_id, user_id, biller_name, biller_nickname, account_number_at_biller, default_payment_account_id, last_due_amount, last_due_date, bill_type) VALUES
('biller_p_inet_001', 'user_priya_002', 'WebNet Inc.', 'Home Internet', 'WN-123456789', 'acc_priya_curr_001', NULL, NULL, 'internet');
INSERT INTO hdfc_voice_assistant.RegisteredBillers (biller_id, user_id, biller_name, biller_nickname, account_number_at_biller, default_payment_account_id, last_due_amount, last_due_date, bill_type) VALUES
('biller_m_prop_002', 'Mohit', 'Property Tax Department', 'Property Tax', 'PT-123456789', 'acc_mohit_curr_001', 1500.00, DATE('2025-06-30'), 'property_tax');