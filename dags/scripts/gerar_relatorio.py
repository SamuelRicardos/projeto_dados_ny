import os
import pandas as pd

# Caminho base para o Data Lake
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def gerar_relatorio():
    trusted_path = os.path.join(BASE_PATH, 'trusted', 'dados_covid_ny.csv')
    business_path = os.path.join(BASE_PATH, 'business', 'relatorio_covid_ny.csv')

    # Criando diretório business se não existir
    os.makedirs(os.path.join(BASE_PATH, 'business'), exist_ok=True)

    if os.path.exists(trusted_path):
        df = pd.read_csv(trusted_path)

        # ✅ Converter 'date_of_interest' para datetime
        df['date_of_interest'] = pd.to_datetime(df['date_of_interest'], errors='coerce')

        # 📊 Criando um resumo estatístico
        resumo = df.groupby(df['date_of_interest'].dt.to_period("M")).agg(
            total_casos=('case_count', 'sum'),
            total_hospitalizacoes=('hospitalized_count', 'sum')
        ).reset_index()

        # Salvando o relatório final na camada business
        resumo.to_csv(business_path, index=False)
        print(f"Relatório gerado na camada 'business': {business_path}")
    else:
        print("Arquivo não encontrado na camada 'trusted'")