# Projeto de Análise de Dados com Airflow e Pandas

Este projeto foi desenvolvido para realizar a análise de dados utilizando **Apache Airflow**, **Pandas**, **Requests** e **JSON**. A análise foi feita a partir de dados recebidos por meio do **Google Colab**, onde usei o **Pandas** para manipulação e análise dos dados.

🔗Link do Google Colab: https://colab.research.google.com/drive/1qYRMqU3ATxLmC_uXcxJp2NnKTUELjmXd?usp=sharing

## Tecnologias Utilizadas:
- **Apache Airflow**: Para orquestrar o fluxo de trabalho e agendamento de tarefas.
- **Pandas**: Para análise e manipulação de dados.
- **Requests**: Para fazer requisições HTTP e acessar dados de APIs.
- **JSON**: Para trabalhar com dados estruturados em formato JSON.

## Como Funciona:
1. Os dados são recebidos através de requisições feitas com o `Requests`.
2. Os dados em formato JSON são processados e analisados utilizando o `Pandas`.
3. O fluxo de trabalho foi orquestrado utilizando o `Apache Airflow`, garantindo o agendamento e execução automática das etapas de análise.

Este projeto demonstra a integração entre ferramentas de análise de dados e orquestração de processos, proporcionando uma visão eficiente dos dados processados.

## Como Executar:
1. Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/seu-repositorio.git
    ```
2. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```
3. Execute o Airflow para iniciar o processo de análise de dados.

---

Esse projeto visa não apenas a análise de dados, mas também a automação de tarefas utilizando o Airflow, possibilitando a execução programada de fluxos de trabalho relacionados a dados.
