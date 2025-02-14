import os
import pandas as pd

# Caminho base para o Data Lake
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def limpar_e_validar_dados():
    raw_path = os.path.join(BASE_PATH, 'raw', 'dados_covid_ny.parquet')
    trusted_path = os.path.join(BASE_PATH, 'trusted', 'dados_covid_ny.parquet')

    # Criando diretório trusted se não existir
    os.makedirs(os.path.join(BASE_PATH, 'trusted'), exist_ok=True)

    if os.path.exists(raw_path):
        # 🔹 Lendo o arquivo Parquet com `pyarrow`
        df = pd.read_parquet(raw_path, engine="pyarrow")

        # 🧹 Removendo colunas desnecessárias (ajuste conforme necessário)
        colunas_para_manter = ['date_of_interest', 'case_count', 'hospitalized_count']
        df = df[colunas_para_manter]

        # 🛠 Convertendo colunas de datas
        df['date_of_interest'] = pd.to_datetime(df['date_of_interest'])

        # 🚀 Removendo valores nulos
        df = df.dropna()

        # 🔹 Salvando os dados limpos no diretório `trusted`
        df.to_parquet(trusted_path, engine="pyarrow", index=False)
        print(f"✅ Dados limpos e salvos na camada 'trusted': {trusted_path}")
    else:
        print("⚠️ Arquivo não encontrado na camada 'raw'")