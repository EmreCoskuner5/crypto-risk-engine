import pandas as pd
import os
from colorama import Fore, Style, init

init(autoreset=True)

csv_path = "trade_history.csv"

if not os.path.exists(csv_path):
    print(f"{Fore.YELLOW}[!] Henüz hiç işlem geçmişi yok. (trade_history.csv bulunamadı){Style.RESET_ALL}")
    exit(0)

df = pd.read_csv(csv_path)

if df.empty:
    print(f"{Fore.YELLOW}[!] trade_history.csv dosyası boş.{Style.RESET_ALL}")
    exit(0)

total_trades = len(df)
wins = len(df[df['PnL'] > 0])
losses = len(df[df['PnL'] <= 0])
win_rate = (wins / total_trades) * 100
total_pnl = df['PnL'].sum()

print(f"\n{Fore.CYAN}╔══════════════════════════════════════════════╗")
print(f"║  {Fore.WHITE}📊 İŞLEM İSTATİSTİKLERİ (TRADE STATS) {Fore.CYAN}      ║")
print(f"╚══════════════════════════════════════════════╝{Style.RESET_ALL}\n")

print(f"  Toplam Kapanan İşlem : {total_trades}")
print(f"  Başarılı İşlem (Kâr) : {Fore.GREEN}{wins}{Style.RESET_ALL}")
print(f"  Başarısız İşlem (Zarar): {Fore.RED}{losses}{Style.RESET_ALL}")
print(f"  Kazanma Oranı (Win Rate): {Fore.YELLOW}%{win_rate:.2f}{Style.RESET_ALL}")
print(f"  Toplam Net PnL       : {Fore.GREEN if total_pnl >= 0 else Fore.RED}${total_pnl:.2f}{Style.RESET_ALL}")
print(f"\n{Fore.CYAN}Son 5 İşlem:{Style.RESET_ALL}")
print(df.tail(5)[['Timestamp', 'Symbol', 'Direction', 'PnL', 'Reason']].to_string(index=False))
print("\n")
