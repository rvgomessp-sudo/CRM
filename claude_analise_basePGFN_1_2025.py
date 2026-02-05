import pandas as pd
import os

os.chdir(r"C:\Rodrigo\BasePGFN\2025")

# Verifica o nome correto do arquivo
print("Arquivos na pasta:", os.listdir())

# Ajusta o nome conforme aparecer (csv ou vsv)
ARQUIVO = "arquivo_lai_SIDA_1_202512.csv"  # ou .vsv se for o caso

df = pd.read_csv(ARQUIVO, sep=";", encoding="latin1", nrows=100000, low_memory=False)
df.to_csv("amostra_100k.csv", sep=";", index=False, encoding="utf-8-sig")
print(f"Amostra salva: {len(df):,} linhas")