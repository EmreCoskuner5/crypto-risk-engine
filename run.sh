#!/bin/bash
echo "[+] Uyku engelleme aktif ediliyor (System Sleep, Display Sleep, Idle Sleep)"
echo "[+] Canlı işlem botu başlatılıyor..."

caffeinate -d -i -s venv/bin/python -m atomic_trader.cli live-trade --interval=10
