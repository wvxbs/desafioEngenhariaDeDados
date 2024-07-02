from pokemon import Pokemon
from databaseQueries import DatabaseQueries
from dataframeManagement import partitionDataframesByPokemonType

def main():
    baseUrl = "https://pokeapi.co/api/v2/pokemon"

    cacheDirectory = "/app/data/cache/"
    databaseDirectory = "/app/data/duckdb/"
    dataframesDirectory = "/app/data/dataFrames/"

    cacheDuration = 7200

    Pokemon(baseUrl, cacheDirectory, cacheDuration, dataframesDirectory)
    DatabaseQueries(databaseDirectory, dataframesDirectory)

    partitionDataframesByPokemonType(dataframesDirectory)

main()