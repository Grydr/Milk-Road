import pandas as pd
import random
import string

def generate_bitcoin_address():
    # Format: 1 atau 3 diikuti oleh 26-34 karakter alphanumeric
    prefix = random.choice(['1', '3'])
    length = random.randint(26, 34)
    characters = string.ascii_letters + string.digits
    address = prefix + ''.join(random.choice(characters) for _ in range(length-1))
    return address

# Baca product.csv dan ambil unique seller_ids
product_df = pd.read_csv('product.csv')
unique_sellers = product_df['seller_id'].unique()

# Buat dictionary untuk data
seller_data = {
    'seller_id': [],
    'bitcoin_address': []
}

# Generate data untuk setiap seller
for seller_id in unique_sellers:
    seller_data['seller_id'].append(seller_id)
    seller_data['bitcoin_address'].append(generate_bitcoin_address())

# Buat DataFrame dan simpan ke CSV
seller_df = pd.DataFrame(seller_data)
seller_df = seller_df.sort_values('seller_id')  # Urutkan berdasarkan seller_id
seller_df.to_csv('seller_addresses.csv', index=False)

# Tampilkan beberapa baris pertama sebagai contoh
print("Contoh beberapa baris dari file yang dihasilkan:")
print(seller_df.head().to_string())
print(f"\nTotal seller yang diproses: {len(unique_sellers)}")