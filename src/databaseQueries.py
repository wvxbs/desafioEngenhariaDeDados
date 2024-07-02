import duckdb
import os
import pyarrow.parquet as pq
import pandas as pd

class DatabaseQueries:
    def __init__(self, databaseDirectory, dataframesDirectory):
        self.databaseDirectory = os.path.join(databaseDirectory, "pokemondb.duckdb")
        self.dataframesDirectory = dataframesDirectory
        self.databaseConnection = self.initializeDatabase()

        self.createTables()
        self.loadDatabaseData()
        self.runQueries()

    def initializeDatabase(self):
        if not os.path.exists(self.databaseDirectory):
            duckdb.connect(database=self.databaseDirectory).close()
        return duckdb.connect(database=self.databaseDirectory)

    def createTables(self):
        try:
            self.databaseConnection.execute("""
                CREATE TABLE IF NOT EXISTS pokemon_moves (
                    id INTEGER,
                    name VARCHAR,
                    move VARCHAR
                )
            """)

            self.databaseConnection.execute("""
                CREATE TABLE IF NOT EXISTS pokemon_types (
                    id INTEGER,
                    name VARCHAR,
                    type VARCHAR
                )
            """)

            self.databaseConnection.execute("""
                CREATE TABLE IF NOT EXISTS pokemon_dim (
                    id INTEGER,
                    name VARCHAR,
                    weight INTEGER,
                    height INTEGER
                )
            """)

        except Exception as e:
            print(f"Erro ao criar tabelas: {e}")

    def loadDatabaseData(self):
        try:
            df_moves = pq.read_table(f"{self.dataframesDirectory}/df_moves.parquet").to_pandas()
            df_types = pq.read_table(f"{self.dataframesDirectory}/df_types.parquet").to_pandas()
            df_dim = pq.read_table(f"{self.dataframesDirectory}/df_dim.parquet").to_pandas()

            self.databaseConnection.execute("DELETE FROM pokemon_moves")
            self.databaseConnection.execute("DELETE FROM pokemon_types")
            self.databaseConnection.execute("DELETE FROM pokemon_dim")

            self.databaseConnection.register("df_moves", df_moves)
            self.databaseConnection.execute("INSERT INTO pokemon_moves SELECT * FROM df_moves")

            self.databaseConnection.register("df_types", df_types)
            self.databaseConnection.execute("INSERT INTO pokemon_types SELECT * FROM df_types")

            self.databaseConnection.register("df_dim", df_dim)
            self.databaseConnection.execute("INSERT INTO pokemon_dim SELECT * FROM df_dim")

        except Exception as e:
            print(f"Erro ao carregar dados no banco de dados: {e}")

    def formatReqOutput(self, df):
        if df.empty: return "Nenhum resultado encontrado."
        
        output = ""
        headers = df.columns.tolist()
        rows = df.head(5).values.tolist()

        column_widths = [max(len(str(value)) for value in column) for column in df.T.values]

        header_row = " | ".join(f"{header:{column_widths[i]}}" for i, header in enumerate(headers))
        separator_row = "-+-".join('-' * column_widths[i] for i in range(len(headers)))

        output += header_row + "\n" + separator_row + "\n"
        for row in rows:
            output += " | ".join(f"{str(value):{column_widths[i]}}" for i, value in enumerate(row)) + "\n"
        
        return output

    def getMostCommomPokemonMoves(self):
        try:
            query = """
                SELECT move, COUNT(*) as move_count
                FROM pokemon_moves
                GROUP BY move
                ORDER BY move_count DESC
            """
            df = self.databaseConnection.execute(query).fetchdf()
            print("a) Cite as 5 habilidades mais recorrentes entre os pokémons:")
            print(self.formatReqOutput(df))
        except Exception as e:
            print(f"Erro ao listar as 5 habilidades mais recorrentes: {e}")

    def getEveryPokemonMove(self):
        try:
            query = """
                SELECT COUNT(DISTINCT move) as distinct_moves_count
                FROM pokemon_moves
            """
            df = self.databaseConnection.execute(query).fetchdf()
            print("b) Quantas habilidades distintas existem no dataset?")
            print(self.formatReqOutput(df))
        except Exception as e:
            print(f"Erro ao contar habilidades distintas: {e}")

    def getMaxPokemonWeight(self):
        try:
            query = """
                SELECT MAX(weight) as max_weight
                FROM pokemon_dim
            """

            max_weight = self.databaseConnection.execute(query).fetchdf().iloc[0]['max_weight']
            query = f"""
                SELECT id, name, weight
                FROM pokemon_dim
                WHERE weight = {max_weight}
            """
            df = self.databaseConnection.execute(query).fetchdf()

            print("c) Qual o maior peso entre os pokémons? E quais são eles?")
            print(self.formatReqOutput(df))

        except Exception as e:
            print(f"Erro ao obter o maior peso entre os pokémons: {e}")

    def getMinPokemonHeight(self):
        try:
            query = """
                SELECT MIN(height) as min_height
                FROM pokemon_dim
            """

            min_height = self.databaseConnection.execute(query).fetchdf().iloc[0]['min_height']
            query = f"""
                SELECT id, name, height
                FROM pokemon_dim
                WHERE height = {min_height}
            """
            df = self.databaseConnection.execute(query).fetchdf()

            print("d) Qual a menor altura entre os pokémons? E quais são eles?")
            print(self.formatReqOutput(df))

        except Exception as e:
            print(f"Erro ao obter a menor altura entre os pokémons: {e}")

    def getAveragePokemonWeight(self):
        try:
            query = """
                SELECT AVG(weight) as avg_weight
                FROM pokemon_dim
            """
            df = self.databaseConnection.execute(query).fetchdf()
            avg_weight = df.iloc[0]['avg_weight']

            print("e) Qual a média de peso entre os pokémons?")
            print(f"{avg_weight:.2f}")

        except Exception as e:
            print(f"Erro ao obter a média de peso entre os pokémons: {e}")

    def getAveragePokemonHeight(self):
        try:
            query = """
                SELECT AVG(height) as avg_height
                FROM pokemon_dim
            """
            df = self.databaseConnection.execute(query).fetchdf()
            avg_height = df.iloc[0]['avg_height']

            print("f) Qual a média de altura entre os pokémons?")
            print(f"{avg_height:.2f}")

        except Exception as e:
            print(f"Erro ao obter a média de altura entre os pokémons: {e}")

    def runQueries(self):
        self.getMostCommomPokemonMoves()
        self.getEveryPokemonMove()
        self.getMaxPokemonWeight()
        self.getMinPokemonHeight()
        self.getAveragePokemonWeight()
        self.getAveragePokemonHeight()