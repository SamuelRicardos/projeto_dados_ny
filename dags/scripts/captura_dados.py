import pandas as pd
import requests
import json
import os

# Caminho base para o Data Lake
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def captura_quantidade_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    
    df = pd.DataFrame(json.loads(response.content))
    
    # Criando diretório da camada landing se não existir
    landing_path = os.path.join(BASE_PATH, 'landing')
    os.makedirs(landing_path, exist_ok=True)
    
    # Salvando os dados em um arquivo CSV dentro da camada landing
    file_path = os.path.join(landing_path, 'dados_covid_ny.csv')
    df.to_csv(file_path, index=False)
    
    print(f"Dados salvos na camada 'landing': {file_path}")

    return len(df.index)  # Retorna a quantidade de linhas
