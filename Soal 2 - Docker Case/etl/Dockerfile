# Gunakan image resmi Python versi 3.11.4 sebagai base image
FROM python:3.11.4

# Set direktori kerja di dalam container ke /app
WORKDIR /app

# Salin semua file dari direktori lokal (host) ke dalam container
COPY . .

# Install dependencies dari requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Jalankan program utama ETL ketika container dimulai
CMD ["python", "etl.py"]
