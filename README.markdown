# **Pipeline de Ingestão de Dados Financeiros**

## **Descrição**
Este projeto implementa um **pipeline de ingestão de dados financeiros** em Python, projetado para **coletar**, **validar**, **processar** e **armazenar** dados históricos de ações em um banco de dados **Oracle**. 

## **Funcionalidades**
- **Coleta de Dados**: Obtém preços de ações (abertura, máxima, mínima, fechamento, volume) via **yfinance** do Yahoo Finance.
- **Validação**: Remove valores ausentes e filtra variações de preço extremas (>50%).
- **Processamento**: Padroniza dados com datas em `YYYY-MM-DD`.
- **Armazenamento**: Insere dados em uma tabela **Oracle** com inserções em lote.
- **Logs**: Registra eventos para monitoramento e depuração.

## **Tecnologias Utilizadas**
- **Python 3.8+**
- **yfinance**: Coleta de dados financeiros.
- **pandas**: Manipulação de dados.
- **numpy**: Cálculos numéricos.
- **oracledb**: Conexão com Oracle.
- **logging**: Registro de eventos.

## **Estrutura do Pipeline**
1. **Conexão**: Estabelece conexão com o **Oracle** e cria a tabela `stock_data`.
2. **Coleta**: Busca dados de tickers (ex.: **AAPL**, **MSFT**, **GOOGL**).
3. **Validação**: Garante qualidade dos dados.
4. **Processamento**: Formata para armazenamento.
5. **Armazenamento**: Insere no **Oracle**.
6. **Encerramento**: Fecha a conexão.

## **Pré-requisitos**
- **Python 3.8+**
- Bibliotecas: `yfinance`, `pandas`, `numpy`, `oracledb`
- Acesso a um banco de dados **Oracle** com credenciais válidas

## **Instalação**
1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/seu-repositorio.git
   cd seu-repositorio
   ```
2. Instale as dependências:
   ```bash
   pip install yfinance pandas numpy oracledb
   ```
3. Configure as credenciais do **Oracle** em `financial_data_pipeline.py`:
   ```python
   user = "seu_usuario"
   password = "sua_senha"
   host = "localhost"
   port = "1521"
   service_name = "seu_service_name"
   ```

## **Como Executar**
1. Execute o script:
   ```bash
   python financial_data_pipeline.py
   ```
2. O pipeline processa dados de ações dos últimos 30 dias e armazena no **Oracle**.
3. Verifique os dados:
   ```sql
   SELECT * FROM stock_data WHERE ticker = 'AAPL';
   ```

## **Estrutura do Banco de Dados**
```sql
CREATE TABLE stock_data (
    date VARCHAR2(10),
    ticker VARCHAR2(10),
    open NUMBER,
    high NUMBER,
    low NUMBER,
    close NUMBER,
    volume NUMBER,
    CONSTRAINT pk_stock_data PRIMARY KEY (date, ticker)
);
```

## **Possíveis Extensões**
- **Indicadores**: Adicionar médias móveis ou RSI.
- **Automação**: Agendamento com **APScheduler**.
- **Visualização**: Gráficos com **matplotlib** ou **plotly**.
- **Outras Fontes**: APIs como **Alpha Vantage**.

## **Contribuições**
Contribuições são bem-vindas! Abra **issues** ou **pull requests** com melhorias.

## **Licença**
Licenciado sob a [MIT License](LICENSE).