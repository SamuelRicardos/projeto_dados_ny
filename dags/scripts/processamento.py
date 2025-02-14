import os
import pandas as pd

# Caminho base para o Data Lake
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def mover_para_raw():
    landing_path = os.path.join(BASE_PATH, 'landing', 'dados_covid_ny.csv')
    raw_path = os.path.join(BASE_PATH, 'raw', 'dados_covid_ny.parquet')

    # Criando diretório raw se não existir
    os.makedirs(os.path.join(BASE_PATH, 'raw'), exist_ok=True)

    if os.path.exists(landing_path):
        # ✅ Lendo CSV antes de mover
        df = pd.read_csv(landing_path)

        # 📝 Convertendo para Parquet
        df.to_parquet(raw_path, engine="pyarrow", index=False)

        # 🗑️ Removendo o arquivo CSV original
        os.remove(landing_path)

        print(f"Arquivo convertido e movido para camada 'raw': {raw_path}")
    else:
        print("Arquivo não encontrado na camada 'landing'")