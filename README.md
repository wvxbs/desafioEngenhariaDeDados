# Desafio de Engenharia de Dados

O código coleta dados da Poke API, armazena-os em cache, salva-os em arquivos Parquet e realiza consultas com SQL.

## Estrutura do Projeto

- `index.py`: Arquivo principal para iniciar o programa.
- `pokemon.py`: Coleta dados da Poke API.
- `dataframeManagement.py`: Criação e salvamento dos DataFrames.
- `cacheManagement.py`: Gerenciamento de cache.
- `databaseQueries.py`: Criação de tabelas e execução de consultas em DuckDB.
- `data/`: Diretório para armazenar cache, arquivos Parquet e banco de dados DuckDB, caso opte por rodar o código localmente.

## Pré-requisitos

- Python 3.12+
- Dependências listadas em `requirements.txt`
- Docker (opcional)

## Instalação

### Clonando o Repositório

1. Clone o repositório:
    ```bash
    git clone https://github.com/wvxbs/desafioEngenhariaDeDados
    cd desafioEngenhariaDeDados
    ```

## Execução
### Utilizando Docker
1. Execute o comando:
    ```bash
    docker-compose up
    ```

### Rodando Localmente
1. Crie um ambiente virtual e ative-o:
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/MacOS
    venv\Scripts\activate  # Windows
    ```

2. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

3. Certifique-se de que os diretórios `data/cache`, `data/dataFrames` e `data/duckdb` existam. Caso não existam, crie-os:
    ```bash
    mkdir -p data/cache data/dataFrames data/duckdb
    ```
4. Faça a seguinte alteração no arquivo `index.py`:
    ```bash
    cacheDirectory = "./data/cache/"
    databaseDirectory = "./data/duckdb/"
    dataframesDirectory = "./data/dataFrames/"
    ```
5. Execute o programa:
    ```bash
    python index.py
    ```

## Funcionalidades

- Coleta dados sobre Pokémon da API PokeAPI.
- Armazena os dados em cache para evitar requisições repetidas.
- Salva os dados em arquivos Parquet para uso posterior.
- Carrega os dados salvos em DuckDB e executa consultas com SQL.


## Consultas Realizadas

1. **Top 5 Habilidades (`moves`) Mais Comuns**:
    ```sql
    SELECT move, COUNT(*) as move_count
    FROM pokemon_moves
    GROUP BY move
    ORDER BY move_count DESC
    ```

2. **Número de Habilidades (`moves`) Distintas**:
    ```sql
    SELECT COUNT(DISTINCT move) as distinct_moves_count
    FROM pokemon_moves
    ```

3. **Pokémon com Maior Peso**:
    ```sql
    SELECT id, name, weight
    FROM pokemon_dim
    WHERE weight = (SELECT MAX(weight) FROM pokemon_dim)
    ```

4. **Pokémon com Menor Altura**:
    ```sql
    SELECT id, name, height
    FROM pokemon_dim
    WHERE height = (SELECT MIN(height) FROM pokemon_dim)
    ```

5. **Média de Peso dos Pokémon**:
    ```sql
    SELECT AVG(weight) as avg_weight
    FROM pokemon_dim
    ```

6. **Média de Altura dos Pokémon**:
    ```sql
    SELECT AVG(height) as avg_height
    FROM pokemon_dim
    ```