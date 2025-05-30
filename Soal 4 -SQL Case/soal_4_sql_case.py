# -*- coding: utf-8 -*-
"""Soal 4 -SQL Case
"""

# NTX Data Cleaning & SQL Analysis Script

# --- Library Setup ---
!pip install pandas sqlalchemy openpyxl pandasql

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import pandasql as psql

# --- Load Dataset ---
data_url = 'https://raw.githubusercontent.com/hafidzwibowo/ntx-de-technical-test/refs/heads/main/Soal%201%20-%20Data%20Transformation%20dan%20Analysis%20Case/ecommerce-session-bigquery.csv'
df = pd.read_csv(data_url)

# --- Data Preview ---
print("=== Kolom 1-10 ===")
display(df.iloc[:, 0:10].head())

print("=== Kolom 11-20 ===")
display(df.iloc[:, 10:20].head())

print("=== Kolom 21-32 ===")
display(df.iloc[:, 20:32].head())

# --- Data Type Conversion ---
# Tujuan: memastikan semua tipe data sesuai untuk analisis dan transformasi.
str_cols = ['fullVisitorId', 'channelGrouping', 'country', 'city', 'date', 'type', 'productSKU', 'v2ProductName',
            'v2ProductCategory', 'productVariant', 'currencyCode', 'transactionId', 'pageTitle',
            'searchKeyword', 'pagePathLevel1', 'eCommerceAction_type', 'eCommerceAction_option']
float_cols = ['time', 'totalTransactionRevenue', 'transactions', 'timeOnSite', 'pageviews', 'sessionQualityDim',
              'visitId', 'productRefundAmount', 'productQuantity', 'productPrice', 'productRevenue',
              'itemQuantity', 'itemRevenue', 'transactionRevenue', 'eCommerceAction_step']

for col in str_cols:
    if col in df.columns:
        df[col] = df[col].astype(str)

for col in float_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# --- Isi Nilai Kosong dengan 0 (Untuk Kolom Numerik Tertentu) ---
# Alasan: logika bisnis menganggap nilai kosong = tidak terjadi (transaksi, revenue, dll)
fill_zero_cols = [
    'totalTransactionRevenue', 'transactions', 'timeOnSite', 'pageviews', 'sessionQualityDim',
    'productRefundAmount', 'productQuantity', 'itemQuantity', 'itemRevenue', 'productRevenue',
    'transactionRevenue'
]

for col in fill_zero_cols:
    if col in df.columns:
        df[col] = df[col].fillna(0)

# --- Isi Nilai Kosong pada Kolom Kategorikal dengan NaN ---
# Alasan: beberapa kolom lebih bermakna jika dibiarkan NaN daripada 0 atau string kosong
fill_na_cols = {
    'channelGrouping': np.nan,
    'country': np.nan,
    'city': np.nan,
    'type': np.nan,
    'productSKU': np.nan,
    'v2ProductName': np.nan,
    'v2ProductCategory': np.nan,
    'productVariant': np.nan,
    'currencyCode': np.nan,
    'transactionId': np.nan,
    'pageTitle': np.nan,
    'searchKeyword': np.nan,
    'pagePathLevel1': np.nan,
    'eCommerceAction_type': np.nan,
    'eCommerceAction_option': np.nan
}

for col, val in fill_na_cols.items():
    if col in df.columns:
        df[col] = df[col].fillna(val)

# --- Filter Baris dengan Harga Produk 0 ---
# Alasan: dianggap tidak relevan atau anomali untuk analisis transaksi
df = df[df['productPrice'] != 0]

import pandas as pd

# Misal df adalah DataFrame yang berisi data
# Pertama, ganti (not set) menjadi "Unknown"
df['country'] = df['country'].replace('(not set)', 'Unknown')

# 1. U.S. Virgin Islands diganti menjadi United States
df['country'] = df['country'].replace('U.S. Virgin Islands', 'United States')

# 2. Aruba diganti menjadi United States
df['country'] = df['country'].replace('Aruba', 'United States')

# 3. Macedonia (FYROM) diganti menjadi North Macedonia
df['country'] = df['country'].replace('Macedonia (FYROM)', 'North Macedonia')

# 4. Bosnia & Herzegovina diganti menjadi Bosnia and Herzegovina
df['country'] = df['country'].replace('Bosnia & Herzegovina', 'Bosnia and Herzegovina')

# 5. Curaçao diganti menjadi Netherlands (jika memang ingin digabungkan dengan Belanda)
df['country'] = df['country'].replace('Curaçao', 'Netherlands')

# Tampilkan hasil perubahan
print(df['country'].unique())

# --- Normalisasi Nama Produk ---
df['v2ProductName'] = df['v2ProductName'].replace({
    '7&quot; Dog Frisbee': '7 Dog Frisbee',
    '7" Dog Frisbee': '7 Dog Frisbee'})

# Tampilkan hasil perubahan
print(df['v2ProductName'].unique())

# --- SQL Query Setup ---
pysqldf = lambda q: psql.sqldf(q, globals())

# === CASE 1 ===
# Objective: Top 5 channel grouping by revenue for the top country
query_case1 = """
WITH top_country AS (
    SELECT country, SUM(totalTransactionRevenue) AS total_revenue
    FROM df
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 1
)
SELECT
    channelGrouping,
    df.country,
    SUM(totalTransactionRevenue) AS total_revenue
FROM df
JOIN top_country ON df.country = top_country.country
GROUP BY channelGrouping, df.country
ORDER BY total_revenue DESC
LIMIT 5;
"""

result_case1 = pysqldf(query_case1)
result_case1['total_revenue'] = result_case1['total_revenue'].apply(lambda x: f"{x:,.0f}")
print(result_case1)

# === CASE 2 ===
# Objective: Identify users who spent a long time on site but viewed few pages
# Step: Replace 0 with mean (non-zero only) to avoid skew
for col in ['timeOnSite', 'pageviews', 'sessionQualityDim']:
    nonzero_mean = df[df[col] != 0][col].mean()
    df[col] = df[col].replace(0, nonzero_mean)

query_case2 = """
WITH user_avg AS (
    SELECT
        fullVisitorId,
        AVG(timeOnSite) AS avg_time,
        AVG(pageviews) AS avg_views,
        AVG(sessionQualityDim) AS avg_quality
    FROM df
    GROUP BY fullVisitorId
),
overall_avg AS (
    SELECT
        AVG(timeOnSite) AS mean_time,
        AVG(pageviews) AS mean_views
    FROM df
)
SELECT
    ua.fullVisitorId,
    ua.avg_time,
    ua.avg_views,
    ua.avg_quality
FROM user_avg ua
CROSS JOIN overall_avg oa
WHERE ua.avg_time > oa.mean_time AND ua.avg_views < oa.mean_views
ORDER BY ua.avg_time DESC;
"""

result_case2 = pysqldf(query_case2)
print(result_case2.head())

# === CASE 3 ===
# Objective: Top 10 products by net revenue, flag those with high refund
query_case3 = """
SELECT
    v2ProductName,
    SUM(productRevenue) AS total_revenue,
    SUM(productQuantity) AS total_qty,
    SUM(productRefundAmount) AS total_refund,
    SUM(productRevenue) - SUM(productRefundAmount) AS net_revenue,
    CASE
        WHEN SUM(productRefundAmount) > 0.1 * SUM(productRevenue) THEN 'FLAGGED'
        ELSE 'OK'
    END AS refund_flag
FROM df
WHERE v2ProductName IS NOT NULL
GROUP BY v2ProductName
ORDER BY net_revenue DESC
LIMIT 10;
"""

result_case3 = pysqldf(query_case3)
print(result_case3)





