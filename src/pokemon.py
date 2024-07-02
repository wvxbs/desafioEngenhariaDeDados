import requests
import time
import pandas as pd
from cacheManagement import loadCache, saveCache
from dataframeManagement import createDataframes

class Pokemon:
    def __init__(self, baseUrl, cacheDirectory, cacheDuration, dataframesDirectory):
        self.baseUrl = baseUrl
        self.offset = 0
        self.limit = 20
        self.cacheDirectory = cacheDirectory
        self.cacheDuration = cacheDuration
        self.dataframesDirectory = dataframesDirectory
        self.cache, self.cacheTimestamp = loadCache(self.cacheDirectory)
        self.allPokemonData = self.cache.get('data', [])
        self.dataFromCache = self.isCacheValid()

        if self.dataFromCache:
            print(f"Dados carregados do cache {self.cacheTimestamp}")
        else:
            self.fetchEveryPokemon()
            saveCache(self.allPokemonData, self.cacheDirectory)
    
    def isCacheValid(self):
        return self.cacheTimestamp and (time.time() - self.cacheTimestamp < self.cacheDuration)
    
    def fetchData(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            return None

    def parsePageData(self, data):
        return data.get("results", [])
    
    def fetchSinglePokemonData(self, pokemonUrl):
        pokemonId = pokemonUrl.rstrip('/').split('/')[-1]
        pokemonData = self.fetchData(pokemonUrl)
        return pokemonData, pokemonId
    
    def fetchEveryPokemon(self):
        print("Requisitando a lista completa de pokémons...")

        run = True

        while run:
            completeUrl = f"{self.baseUrl}?offset={self.offset}&limit={self.limit}"
            pageData = self.fetchData(completeUrl)
            if pageData:
                self.allPokemonData.extend(self.parsePageData(pageData))
                for pokemon in pageData["results"]:
                    pokemonData, pokemonId = self.fetchSinglePokemonData(pokemon["url"])
                    createDataframes(pokemonData, pokemonId, self.dataframesDirectory)

                if pageData.get("next"):
                    self.offset += self.limit
                else:
                    run = False
            else:
                run = False
        
    def getAllPokemon(self):
        return self.allPokemonData