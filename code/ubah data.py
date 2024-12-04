import pandas as pd

# Baca file product.csv
product_df = pd.read_csv('product.csv')
product_seller_ids = set(product_df['seller_id'])

# Baca file seller_id.txt
with open('seller_id.txt', 'r') as file:
    seller_data = file.read()

# Proses setiap baris
processed_lines = []
for line in seller_data.split('\n'):
    if line.strip():  # Skip baris kosong
        seller_id = int(line.split()[-1])  # Ambil angka terakhir dari setiap baris
        if seller_id in product_seller_ids:
            processed_lines.append(str(seller_id))

# Tampilkan hasil
print("\n".join(processed_lines))

# Opsional: Simpan hasil ke file
with open('processed_sellers.txt', 'w') as file:
    file.write("\n".join(processed_lines))