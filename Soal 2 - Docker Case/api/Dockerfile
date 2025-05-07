# Gunakan image resmi Python versi 3.11.4 sebagai base image
FROM python:3.11.4

# Set direktori kerja di dalam container ke /app
WORKDIR /app

# Salin semua file dari direktori lokal (host) ke dalam container
COPY . .

# Install semua dependencies dari requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Jalankan API dengan uvicorn pada host 0.0.0.0 dan port 6000
# 'api:app' artinya: dari file api.py, jalankan variabel 'app'
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "6000"]
