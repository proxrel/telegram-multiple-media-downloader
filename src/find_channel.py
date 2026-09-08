"""
Kanal/grup ID'sini bulmak icin yardimci script.

Bu script, giris yaptigin Telegram hesabindaki tum sohbetleri (kanal, grup,
kisi) isim ve ID'leriyle birlikte listeler. Indirmek istedigin kanalin ID'sini
bulmak icin kullanabilirsin.

Kullanim:
    1. Proje kok dizininde bir .env dosyasi olustur (bkz. .env.example) ve
       API_ID / API_HASH degerlerini gir.
    2. `python find_channel.py` calistir.
    3. Ciktida kanal adinin yanindaki ID degerini not al.

Belirli bir kanali aramak istersen TARGET_NAME degiskenini o kanalin adiyla
degistirebilirsin; bos birakirsan tum sohbetler listelenir.
"""

import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "default_session")

# Aramak istedigin kanal/grup adi (opsiyonel). Bos birakirsan hepsi listelenir.
TARGET_NAME = ""


async def main():
    if not API_ID or not API_HASH:
        print("API_ID ve API_HASH bulunamadi. Once .env dosyasini olustur.")
        return

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    await client.start()

    print("Giris basarili. Kanallar/gruplar listeleniyor...\n")

    found = False
    dialogs = await client.get_dialogs()
    for d in dialogs:
        print(f"AD: {d.name} | ID: {d.id}")
        if TARGET_NAME and d.name == TARGET_NAME:
            found = True
            print("\nBULUNDU!")
            print(f"KANAL/GRUP ADI: {d.name}")
            print(f"KANAL/GRUP ID: {d.id}\n")

    if TARGET_NAME and not found:
        print(f"\n'{TARGET_NAME}' listede bulunamadi.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
