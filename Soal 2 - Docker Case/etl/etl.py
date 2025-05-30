import asyncio
import httpx
import time

time.sleep(5)  # Tunggu 5 detik agar API container siap menerima request

# Daftar kalimat yang akan dikirim ke API untuk diproses
sentences = [
    "Saya suka makan nasi goreng.",
    "Hari ini cuaca sangat panas.",
    "Anjing itu sangat lucu.",
    "Kami akan pergi ke pantai besok.",
    "Apakah kamu punya hobi?",
    "Buku ini sangat menarik untuk dibaca.",
    "Sekarang waktunya istirahat.",
    "Kucing itu tidur di bawah meja.",
    "Makanan favorit saya adalah rendang.",
    "Kami berencana untuk berlibur ke Bali.",
]

# URL endpoint API (menggunakan nama service dari docker-compose)
api_url = "http://api:6000/predict"

# Fungsi utama untuk kirim POST request secara asynchronous
async def main():
    for sentence in sentences:
        try:
            async with httpx.AsyncClient() as aclient:
                response = await aclient.post(api_url, params={"text": sentence})
                response.raise_for_status()
                print(response.json())  # Cetak hasil prediksi dari API
        except Exception as e:
            print(e)  # Tangani error jika API tidak merespon
            continue

# Jalankan fungsi main saat script dipanggil
if __name__ == "__main__":
    asyncio.run(main())
