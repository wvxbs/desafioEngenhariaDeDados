import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def createDataframes(pokemonData, pokemonId, dataframesDirectory):
    df_moves = pd.DataFrame(columns=["id", "name", "move"])
    df_types = pd.DataFrame(columns=["id", "name", "type"])
    df_dim = pd.DataFrame(columns=["id", "name", "weight", "height"])

    moves = pokemonData.get("moves", [])
    types = pokemonData.get("types", [])
    weight = pokemonData.get("weight", "")
    height = pokemonData.get("height", "")

    moveNames = [move["move"]["name"] for move in moves]
    typeNames = [type["type"]["name"] for type in types]

    pokemonName = pokemonData.get("name", "")

    df_movesRows = pd.DataFrame({"id": [pokemonId] * len(moveNames), "name": [pokemonName] * len(moveNames), "move": moveNames})
    df_typesRows = pd.DataFrame({"id": [pokemonId] * len(typeNames), "name": [pokemonName] * len(typeNames), "type": typeNames})
    df_dimRow = pd.DataFrame({"id": [pokemonId], "name": [pokemonName], "weight": [weight], "height": [height]})

    df_moves = pd.concat([df_moves, df_movesRows], ignore_index=True)
    df_types = pd.concat([df_types, df_typesRows], ignore_index=True)
    df_dim = pd.concat([df_dim, df_dimRow], ignore_index=True)

    saveDataframes(df_moves, df_types, df_dim, dataframesDirectory)

def saveDataframes(df_moves, df_types, df_dim, dataframesDirectory):
    try:
        os.makedirs(dataframesDirectory, exist_ok=True)
        
        def LoadOrCreateDataframe(filepath, columns):
            if os.path.exists(filepath):
                return pq.read_table(filepath).to_pandas()
            else:
                return pd.DataFrame(columns=columns)

        moves_path = f"{dataframesDirectory}/df_moves.parquet"
        types_path = f"{dataframesDirectory}/df_types.parquet"
        dim_path = f"{dataframesDirectory}/df_dim.parquet"

        df_moves_existing = LoadOrCreateDataframe(moves_path, df_moves.columns)
        df_types_existing = LoadOrCreateDataframe(types_path, df_types.columns)
        df_dim_existing = LoadOrCreateDataframe(dim_path, df_dim.columns)

        df_moves_combined = pd.concat([df_moves_existing, df_moves], ignore_index=True)
        df_types_combined = pd.concat([df_types_existing, df_types], ignore_index=True)
        df_dim_combined = pd.concat([df_dim_existing, df_dim], ignore_index=True)

        pq.write_table(pa.Table.from_pandas(df_moves_combined), moves_path)
        pq.write_table(pa.Table.from_pandas(df_types_combined), types_path)
        pq.write_table(pa.Table.from_pandas(df_dim_combined), dim_path)

    except Exception as e:
        print(f"Erro ao salvar os DataFrames: {e}")

def partitionDataframesByPokemonType(dataframesDirectory):
    try:
        df_types = pq.read_table(f"{dataframesDirectory}/df_types.parquet").to_pandas()
        uniqueTypes = df_types['type'].unique()

        for type in uniqueTypes:
            df_filtered = df_types[df_types['type'] == type]
            path = f"{dataframesDirectory}/tipos/{type}/pokemons_{type}.parquet"
            
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df_filtered.to_parquet(path, index=False)

    except Exception as e:
        print(f"Erro ao particionar DataFrame df_types por tipo: {e}")
