from fastapi import FastAPI
import hashlib

# Inisialisasi aplikasi FastAPI
app = FastAPI()

# Endpoint /predict yang menerima teks dan mengembalikan hasil hash modulo 4
@app.post("/predict")
async def predict(text: str) -> int:
    hashed = int(hashlib.md5(text.encode()).hexdigest(), 16)  # Hash string jadi angka
    result = hashed % 4  # Ambil modulo 4 sebagai hasil klasifikasi
    return result

# Menjalankan server FastAPI jika file ini dijalankan langsung
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6000)  # Terima koneksi dari luar container
