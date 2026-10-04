# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#  Create Dim_Customers Table (SCD Type 2)

# Read Silver tables
customers_silver = spark.table("banking_catalog.silver.customers")
accounts_silver = spark.table("banking_catalog.silver.accounts")
transactions_silver = spark.table("banking_catalog.silver.transactions")
loans_silver = spark.table("banking_catalog.silver.loans")
fraud_silver = spark.table("banking_catalog.silver.fraud_alerts")
atm_silver = spark.table("banking_catalog.silver.atm_transactions")
branches_silver = spark.table("banking_catalog.silver.branches")


# Create Dim_Customers with SCD Type 2
dim_customers = customers_silver \
    .withColumn("customer_sk", monotonically_increasing_id()) \
    .withColumn("effective_date", current_date()) \
    .withColumn("end_date", lit("9999-12-31").cast("date")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("full_name", concat(col("name"), lit(" (ID: "), col("customer_id"), lit(")"))) \
    .withColumn("age", datediff(current_date(), col("dob")) / 365.25) \
    .withColumn("age_group", 
        when(col("age") < 30, "Young")
        .when(col("age") < 50, "Middle")
        .otherwise("Senior"))


# Write to Gold
dim_customers.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_customers")


print(f"Dim_Customers: {dim_customers.count()} records")

# COMMAND ----------

#  Create Dim_Accounts Table

dim_accounts = accounts_silver \
    .withColumn("account_sk", monotonically_increasing_id()) \
    .withColumn("effective_date", current_date()) \
    .withColumn("is_current", lit(True)) \
    .withColumn("account_age_days", datediff(current_date(), col("date"))) \
    .withColumn("balance_category",
        when(col("amount") < 10000, "Low")
        .when(col("amount") < 50000, "Medium")
        .otherwise("High"))


# Write to Gold
dim_accounts.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_accounts")


print(f"Dim_Accounts: {dim_accounts.count()} records")


# COMMAND ----------

#  Create Dim_Date Table
def create_date_dimension(start_date, end_date):
    dates = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(
            TO_DATE('{start_date}'), 
            TO_DATE('{end_date}'), 
            INTERVAL 1 DAY
        )) AS date
    """)
    
    date_dim = dates.select(
        col("date"),
        year("date").alias("year"),
        quarter("date").alias("quarter"),
        month("date").alias("month"),
        dayofmonth("date").alias("day"),
        dayofweek("date").alias("day_of_week"),
        date_format("date", "EEEE").alias("day_name"),
        date_format("date", "MMMM").alias("month_name"),
        when(dayofweek("date").isin([1, 7]), "Weekend").otherwise("Weekday").alias("weekday_indicator")
    ).withColumn("date_sk", monotonically_increasing_id())
    
    return date_dim


date_dim_df = create_date_dimension("2020-01-01", "2025-12-31")


date_dim_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_date")


print(f"Dim_Date: {date_dim_df.count()} records")


# COMMAND ----------

# Create Fact_Transactions

fact_transactions = transactions_silver \
    .join(dim_accounts.select("account_id", "account_sk"), "account_id", "left") \
    .join(dim_customers.select("customer_id", "customer_sk"), "customer_id", "left") \
    .join(date_dim_df.select("date", "date_sk"), transactions_silver["date"] == date_dim_df["date"], "left") \
    .select(
        "transaction_id",
        "customer_sk",
        "account_sk",
        "date_sk",
        "amount",
        "status",
        col("type").alias("transaction_type"),
        "branch",
        "remarks",
        "flag",
        "year",
        "month",
        "quarter"
    )


# Write to Gold
fact_transactions.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_transactions")


print(f"Fact_Transactions: {fact_transactions.count()} records")


# COMMAND ----------

# Create Fact_Loans

fact_loans = loans_silver \
    .join(dim_accounts.select("account_id", "account_sk"), "account_id", "left") \
    .join(dim_customers.select("customer_id", "customer_sk"), "customer_id", "left") \
    .join(date_dim_df.select("date", "date_sk"), loans_silver["date"] == date_dim_df["date"], "left") \
    .select(
        "loan_id",
        "customer_sk",
        "account_sk",
        "date_sk",
        "amount",
        "status",
        col("type").alias("loan_type"),
        "branch",
        "remarks",
        "flag"
    )


# Write to Gold
fact_loans.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_loans")


print(f"Fact_Loans: {fact_loans.count()} records")


# COMMAND ----------

# Create Fact_Fraud_Alerts
fact_fraud = fraud_silver \
    .join(dim_customers.select("customer_id", "customer_sk"), "customer_id", "left") \
    .join(dim_accounts.select("account_id", "account_sk"), "account_id", "left") \
    .join(date_dim_df.select("date", "date_sk"), fraud_silver["date"] == date_dim_df["date"], "left") \
    .select(
        "alert_id",
        "customer_sk",
        "account_sk",
        "date_sk",
        "amount",
        "status",
        col("type").alias("alert_type"),
        "branch",
        "remarks",
        "flag",
        "is_high_risk"
    )


# Write to Gold
fact_fraud.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_fraud_alerts")


print(f"Fact_Fraud_Alerts: {fact_fraud.count()} records")




# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold tables in unity catalog
# MAGIC CREATE TABLE banking_catalog.gold.dim_customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_customers/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.gold.dim_accounts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_accounts/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.gold.dim_date
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/dim_date/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.gold.fact_transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_transactions/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.gold.fact_loans
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_loans/';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE banking_catalog.gold.fact_fraud_alerts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stbankinglakeguna.dfs.core.windows.net/fact_fraud_alerts/';
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #Business Problems
# MAGIC -- # Total Balance by Account Type
# MAGIC SELECT 
# MAGIC     type,
# MAGIC     COUNT(*) as account_count,
# MAGIC     SUM(amount) as total_balance,
# MAGIC     AVG(amount) as avg_balance,
# MAGIC     MIN(amount) as min_balance,
# MAGIC     MAX(amount) as max_balance
# MAGIC FROM banking_catalog.silver.accounts
# MAGIC WHERE amount IS NOT NULL
# MAGIC GROUP BY type
# MAGIC ORDER BY total_balance DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Customer KYC Status Analysis
# MAGIC SELECT 
# MAGIC     kyc_status,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM banking_catalog.silver.customers
# MAGIC GROUP BY kyc_status
# MAGIC ORDER BY percentage DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly Transaction Trends
# MAGIC SELECT 
# MAGIC     YEAR(date) as year,
# MAGIC     MONTH(date) as month,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY YEAR(date), MONTH(date)
# MAGIC ORDER BY year DESC, month DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 Customers by Transaction Volume
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     COUNT(t.transaction_id) as transaction_count,
# MAGIC     SUM(t.amount) as total_transaction_amount,
# MAGIC     AVG(t.amount) as avg_transaction
# MAGIC FROM banking_catalog.silver.customers c
# MAGIC JOIN banking_catalog.silver.transactions t
# MAGIC ON c.customer_id = t.customer_id
# MAGIC GROUP BY c.customer_id, c.name
# MAGIC ORDER BY total_transaction_amount DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Branch Performance Comparison
# MAGIC SELECT 
# MAGIC     branch,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount,
# MAGIC     COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY branch
# MAGIC ORDER BY total_amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC -- Fraud Alert Status Distribution
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) as alert_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount
# MAGIC FROM banking_catalog.silver.fraud_alerts
# MAGIC GROUP BY status
# MAGIC ORDER BY alert_count DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-Risk Fraud Alerts
# MAGIC SELECT 
# MAGIC     alert_id,
# MAGIC     customer_id,
# MAGIC     amount,
# MAGIC     status,
# MAGIC     type,
# MAGIC     date,
# MAGIC     branch
# MAGIC FROM banking_catalog.silver.fraud_alerts
# MAGIC WHERE amount > 40000
# MAGIC ORDER BY amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Loan Portfolio Status
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) as loan_count,
# MAGIC     SUM(amount) as total_loan_amount,
# MAGIC     AVG(amount) as avg_loan_amount,
# MAGIC     ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER(), 2) as percentage
# MAGIC FROM banking_catalog.silver.loans
# MAGIC GROUP BY status
# MAGIC ORDER BY total_loan_amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Customer Gender Distribution
# MAGIC SELECT 
# MAGIC     COALESCE(gender, 'Unknown') as gender,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM banking_catalog.silver.customers
# MAGIC GROUP BY gender
# MAGIC ORDER BY percentage DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Average Transaction Amount by City
# MAGIC SELECT 
# MAGIC     c.city,
# MAGIC     COUNT(t.transaction_id) as transaction_count,
# MAGIC     ROUND(AVG(t.amount), 2) as avg_transaction_amount,
# MAGIC     ROUND(SUM(t.amount), 2) as total_transaction_amount
# MAGIC FROM banking_catalog.silver.customers c
# MAGIC JOIN banking_catalog.silver.transactions t
# MAGIC ON c.customer_id = t.customer_id
# MAGIC WHERE c.city IS NOT NULL
# MAGIC GROUP BY c.city
# MAGIC ORDER BY avg_transaction_amount DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Active vs Inactive Accounts
# MAGIC
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) as account_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM banking_catalog.silver.accounts
# MAGIC GROUP BY status
# MAGIC ORDER BY percentage DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Customer Lifecycle - Created Date Analysis
# MAGIC SELECT 
# MAGIC     YEAR(created_date) as year,
# MAGIC     MONTH(created_date) as month,
# MAGIC     COUNT(*) as new_customers
# MAGIC FROM banking_catalog.silver.customers
# MAGIC WHERE created_date IS NOT NULL
# MAGIC GROUP BY YEAR(created_date), MONTH(created_date)
# MAGIC ORDER BY year DESC, month DESC
# MAGIC LIMIT 12;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Customers with Both Loans and Accounts
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     c.city
# MAGIC FROM banking_catalog.silver.customers c
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 FROM banking_catalog.silver.accounts a 
# MAGIC     WHERE a.customer_id = c.customer_id AND a.status = 'ACTIVE'
# MAGIC )
# MAGIC AND EXISTS (
# MAGIC     SELECT 1 FROM banking_catalog.silver.loans l 
# MAGIC     WHERE l.customer_id = c.customer_id
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fraudulent Transactions vs Regular Transactions
# MAGIC SELECT 
# MAGIC     'Regular' as transaction_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(AVG(amount), 2) as avg_amount,
# MAGIC     ROUND(SUM(amount), 2) as total_amount
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Fraud' as transaction_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(AVG(amount), 2) as avg_amount,
# MAGIC     ROUND(SUM(amount), 2) as total_amount
# MAGIC FROM banking_catalog.silver.fraud_alerts;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarterly Performance Analysis
# MAGIC
# MAGIC SELECT 
# MAGIC     YEAR(date) as year,
# MAGIC     QUARTER(date) as quarter,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_transactions,
# MAGIC     AVG(amount) as avg_transaction
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY YEAR(date), QUARTER(date)
# MAGIC ORDER BY year DESC, quarter DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Top 5 Branches by Customer Base
# MAGIC
# MAGIC SELECT 
# MAGIC     branch,
# MAGIC     COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC     COUNT(*) as transaction_count
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY branch
# MAGIC ORDER BY unique_customers DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KYC Verification Status by City
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     COUNT(*) as total_customers,
# MAGIC     SUM(CASE WHEN kyc_status = 'Y' THEN 1 ELSE 0 END) as kyc_completed,
# MAGIC     ROUND(SUM(CASE WHEN kyc_status = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as kyc_rate
# MAGIC FROM banking_catalog.silver.customers
# MAGIC WHERE city IS NOT NULL
# MAGIC GROUP BY city
# MAGIC ORDER BY kyc_rate DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Loan Default Risk Analysis
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     COUNT(l.loan_id) as total_loans,
# MAGIC     SUM(l.amount) as total_loan_amount,
# MAGIC     ROUND(AVG(l.amount), 2) as avg_loan_amount,
# MAGIC     MAX(l.amount) as max_loan_amount
# MAGIC FROM banking_catalog.silver.customers c
# MAGIC JOIN banking_catalog.silver.loans l
# MAGIC ON c.customer_id = l.customer_id
# MAGIC WHERE l.status = 'PENDING'
# MAGIC GROUP BY c.customer_id, c.name
# MAGIC HAVING SUM(l.amount) > 100000
# MAGIC ORDER BY total_loan_amount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Daily Transaction Pattern
# MAGIC
# MAGIC SELECT 
# MAGIC     DAYOFWEEK(date) as day_number,
# MAGIC     CASE DAYOFWEEK(date)
# MAGIC         WHEN 1 THEN 'Sunday'
# MAGIC         WHEN 2 THEN 'Monday'
# MAGIC         WHEN 3 THEN 'Tuesday'
# MAGIC         WHEN 4 THEN 'Wednesday'
# MAGIC         WHEN 5 THEN 'Thursday'
# MAGIC         WHEN 6 THEN 'Friday'
# MAGIC         WHEN 7 THEN 'Saturday'
# MAGIC     END as day_name,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     ROUND(AVG(amount), 2) as avg_amount
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY DAYOFWEEK(date)
# MAGIC ORDER BY day_number;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 27
# MAGIC %sql
# MAGIC --  Cross-Sell Analysis - Customers with Multiple Products
# MAGIC
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     COUNT(DISTINCT a.account_id) as total_accounts,
# MAGIC     COUNT(DISTINCT l.loan_id) as total_loans,
# MAGIC     COUNT(DISTINCT f.alert_id) as fraud_alerts
# MAGIC FROM banking_catalog.silver.customers c
# MAGIC LEFT JOIN banking_catalog.silver.accounts a ON c.customer_id = a.customer_id
# MAGIC LEFT JOIN banking_catalog.silver.loans l ON c.customer_id = l.customer_id
# MAGIC LEFT JOIN banking_catalog.silver.fraud_alerts f ON c.customer_id = f.customer_id
# MAGIC GROUP BY c.customer_id, c.name
# MAGIC ORDER BY total_accounts DESC, total_loans DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Wealth Segmentation
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN total_balance < 10000 THEN 'Low (< 10K)'
# MAGIC         WHEN total_balance < 50000 THEN 'Medium (10K - 50K)'
# MAGIC         WHEN total_balance < 100000 THEN 'High (50K - 100K)'
# MAGIC         ELSE 'Very High (> 100K)'
# MAGIC     END as wealth_segment,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     ROUND(AVG(total_balance), 2) as avg_balance,
# MAGIC     ROUND(SUM(total_balance), 2) as total_wealth
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         SUM(amount) as total_balance
# MAGIC     FROM banking_catalog.silver.accounts
# MAGIC     GROUP BY customer_id
# MAGIC ) customer_balances
# MAGIC GROUP BY wealth_segment
# MAGIC ORDER BY wealth_segment;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent Fraud Trends
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('month', date) as month,
# MAGIC     COUNT(*) as fraud_count,
# MAGIC     ROUND(AVG(amount), 2) as avg_fraud_amount,
# MAGIC     SUM(amount) as total_fraud_amount
# MAGIC FROM banking_catalog.silver.fraud_alerts
# MAGIC WHERE date >= DATE_SUB(CURRENT_DATE(), 180)
# MAGIC GROUP BY DATE_TRUNC('month', date)
# MAGIC ORDER BY month DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Acquisition by State
# MAGIC SELECT 
# MAGIC     state,
# MAGIC     COUNT(*) as total_customers,
# MAGIC     SUM(CASE WHEN kyc_status = 'Y' THEN 1 ELSE 0 END) as kyc_verified,
# MAGIC     DATE_TRUNC('year', created_date) as acquisition_year
# MAGIC FROM banking_catalog.silver.customers
# MAGIC WHERE state IS NOT NULL
# MAGIC GROUP BY state, DATE_TRUNC('year', created_date)
# MAGIC ORDER BY acquisition_year DESC, total_customers DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transaction Amount Distribution
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN amount < 1000 THEN 'Micro (< 1K)'
# MAGIC         WHEN amount < 10000 THEN 'Small (1K - 10K)'
# MAGIC         WHEN amount < 50000 THEN 'Medium (10K - 50K)'
# MAGIC         ELSE 'Large (> 50K)'
# MAGIC     END as transaction_segment,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     ROUND(AVG(amount), 2) as avg_amount,
# MAGIC     ROUND(SUM(amount), 2) as total_amount,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM banking_catalog.silver.transactions
# MAGIC GROUP BY transaction_segment
# MAGIC ORDER BY transaction_segment;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Customer Activity Patterns
# MAGIC WITH customer_stats AS (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         COUNT(*) as total_transactions,
# MAGIC         SUM(amount) as total_spent,
# MAGIC         DATEDIFF(CURRENT_DATE(), MAX(date)) as days_since_last_transaction
# MAGIC     FROM banking_catalog.silver.transactions
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN total_transactions >= 100 THEN 'Very Active'
# MAGIC         WHEN total_transactions >= 50 THEN 'Active'
# MAGIC         WHEN total_transactions >= 10 THEN 'Moderate'
# MAGIC         ELSE 'Occasional'
# MAGIC     END as activity_level,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     ROUND(AVG(total_spent), 2) as avg_spent,
# MAGIC     ROUND(AVG(days_since_last_transaction), 2) as avg_days_inactive
# MAGIC FROM customer_stats
# MAGIC GROUP BY activity_level
# MAGIC ORDER BY activity_level;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  YOY -Year-over-Year Analysis - Customer Growth
# MAGIC
# MAGIC SELECT 
# MAGIC     YEAR(created_date) as year,
# MAGIC     COUNT(*) as new_customers,
# MAGIC     LAG(COUNT(*)) OVER (ORDER BY YEAR(created_date)) as prev_year_customers,
# MAGIC     ROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY YEAR(created_date))) * 100.0 / LAG(COUNT(*)) OVER (ORDER BY YEAR(created_date)), 2) as yoy_growth
# MAGIC FROM banking_catalog.silver.customers
# MAGIC WHERE created_date IS NOT NULL
# MAGIC GROUP BY YEAR(created_date)
# MAGIC ORDER BY year;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Optimize all tables for better performance
# MAGIC OPTIMIZE banking_catalog.silver.accounts;
# MAGIC OPTIMIZE banking_catalog.silver.transactions;
# MAGIC OPTIMIZE banking_catalog.silver.loans;
# MAGIC OPTIMIZE banking_catalog.gold.fact_transactions;
# MAGIC OPTIMIZE banking_catalog.gold.fact_loans;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Collect Table Statistics
# MAGIC ANALYZE TABLE banking_catalog.silver.accounts COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE banking_catalog.silver.transactions COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE banking_catalog.gold.fact_transactions COMPUTE STATISTICS;
# MAGIC

# COMMAND ----------

