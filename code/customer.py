import random
import hashlib
import pandas as pd
import string
from datetime import datetime, timedelta

def generate_account_data(num_accounts=50):
    # Sample names for demonstration
    sample_names = [
        "John Smith", "Maria Garcia", "David Lee", "Sarah Johnson", "Mohammed Ahmed",
        "Emma Wilson", "Luis Rodriguez", "Anna Kowalski", "Chen Wei", "Priya Patel", "Ethan Parker",
        "Sophia Ramirez", "Liam Anderson", "Ava Thompson", "Oliver Martinez", "Emma Collins", "Noah Rivera",
        "Mia Bennett", "Lucas Hernandez", "Amelia Johnson", "Mason Cooper", "Isabella Gray", "Jack Stevens", "Charlotte King",
        "Benjamin Walker", "Lily Scott", "Samuel Mitchell", "Grace Carter", "Alexander Morris", "Zoe Harris"
    ]  # Multiply list to get more names
    
    accounts = []
    for name in sample_names[:num_accounts]:
        # Hash the name to create account_id
        account_id = hashlib.sha256(name.encode()).hexdigest()[:10]
        accounts.append({
            'account_id': account_id,
            'name_hash': hashlib.sha256(name.encode()).hexdigest()[:30]  # Full hash stored
            # 'created_at': datetime.now() - timedelta(days=random.randint(0, 365))
        })
    
    return pd.DataFrame(accounts)

def generate_customer_data(accounts_df, num_transactions=200):
    transactions = []
    
    for _ in range(num_transactions):
        # Randomly select an account
        account = accounts_df.sample(n=1).iloc[0]
        
        # Generate customer_id (LOG followed by 4 random digits)
        customer_number = ''.join(random.choices(string.digits, k=4))
        customer_id = f"LOG{customer_number}"
        
        # Generate order_id (ORD followed by 7 random digits)
        order_number = ''.join(random.choices(string.digits, k=7))
        order_id = f"ORD{order_number}"
        
        # Generate random order amount
        order_amount = round(random.uniform(10, 1000), 2)
        
        # Generate random date after account creation
        transaction_date = account['created_at'] + timedelta(days=random.randint(0, 180))
        
        transactions.append({
            'customer_id': customer_id,
            'account_id': account['account_id'],
            'order_id': order_id,
            'order_amount': order_amount,
            'transaction_date': transaction_date
        })
    
    return pd.DataFrame(transactions)

# Generate the data
accounts_df = generate_account_data(num_accounts=50)
transactions_df = generate_customer_data(accounts_df, num_transactions=200)

# Save to CSV
accounts_df.to_csv('accounts.csv', index=False)
transactions_df.to_csv('customers.csv', index=False)

# Display sample queries
print("\nSample database queries (using pandas):\n")

# 1. Find most frequent buyers
frequent_buyers = transactions_df.groupby('account_id').agg({
    'order_id': 'count',
    'order_amount': 'sum'
}).sort_values('order_id', ascending=False)

print("Top 5 Most Frequent Buyers:")
print(frequent_buyers.head())

# 2. Find average order value per account
avg_order_value = transactions_df.groupby('account_id').agg({
    'order_amount': 'mean'
}).sort_values('order_amount', ascending=False)

print("\nTop 5 Highest Average Order Value:")
print(avg_order_value.head())