# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION raw_location
# MAGIC URL 'abfss://raw@stbankinglakeguna.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL banking_credential);
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION bronze_location
# MAGIC URL 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL banking_credential);
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION silver_location
# MAGIC URL 'abfss://silver@stbankinglakeguna.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL banking_credential);
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION gold_location
# MAGIC URL 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL banking_credential);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Catalog
# MAGIC CREATE CATALOG banking_catalog
# MAGIC MANAGED LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/';
# MAGIC
# MAGIC
# MAGIC -- Create Schemas for each layer
# MAGIC CREATE SCHEMA banking_catalog.bronze
# MAGIC MANAGED LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/';
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA banking_catalog.silver
# MAGIC MANAGED LOCATION 'abfss://silver@stbankinglakeguna.dfs.core.windows.net/';
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA banking_catalog.gold
# MAGIC MANAGED LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/';
# MAGIC

# COMMAND ----------

#  Read Raw CSV Data into DataFrames


raw_path = "abfss://raw@stbankinglakeguna.dfs.core.windows.net/"
accounts_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}accounts.csv")
atm_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}atm_transactions.csv")
branches_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}branches.csv")
credit_cards_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}credit_cards.csv")
customers_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}customers.csv")
employees_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}employees.csv")
fraud_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}fraud_alerts.csv")
kyc_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}kyc_documents.csv")
loans_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}loans.csv")
transactions_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw_path}transactions.csv")


print("All CSV files loaded successfully")




# COMMAND ----------

# Write each DataFrame as Delta table in Bronze layer
bronze_path = "abfss://bronze@stbankinglakeguna.dfs.core.windows.net/"


# Accounts
accounts_df.write.format("delta").mode("overwrite").save(f"{bronze_path}accounts")


# ATM Transactions
atm_df.write.format("delta").mode("overwrite").save(f"{bronze_path}atm_transactions")


# Branches
branches_df.write.format("delta").mode("overwrite").save(f"{bronze_path}branches")


# Credit Cards
credit_cards_df.write.format("delta").mode("overwrite").save(f"{bronze_path}credit_cards")


# Customers
customers_df.write.format("delta").mode("overwrite").save(f"{bronze_path}customers")


# Employees
employees_df.write.format("delta").mode("overwrite").save(f"{bronze_path}employees")


# Fraud Alerts
fraud_df.write.format("delta").mode("overwrite").save(f"{bronze_path}fraud_alerts")


# KYC Documents
kyc_df.write.format("delta").mode("overwrite").save(f"{bronze_path}kyc_documents")


# Loans
loans_df.write.format("delta").mode("overwrite").save(f"{bronze_path}loans")


# Transactions
transactions_df.write.format("delta").mode("overwrite").save(f"{bronze_path}transactions")


print("All tables written to Bronze layer!")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze Tables in Unity Catalog
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.accounts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/accounts/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.atm_transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/atm_transactions/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.branches
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/branches/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.credit_cards
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/credit_cards/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/customers/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.employees
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/employees/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.fraud_alerts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/fraud_alerts/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.kyc_documents
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/kyc_documents/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.loans
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/loans/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.bronze.transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@stbankinglakeguna.dfs.core.windows.net/transactions/';
# MAGIC

# COMMAND ----------

