import requests
import time
from cache import loadCache, saveCache

class Pokemon:
    def __init__(self, baseUrl, cacheDirectory, cacheDuration):
        self.baseUrl = baseUrl
        self.offset = 0
        self.limit = 20
        self.cacheDirectory = cacheDirectory
        self.cacheDuration = cacheDuration
        self.cache, self.cacheTimestamp = loadCache(self.cacheDirectory)
        self.allPokemon = self.cache.get('data', [])
        self.dataFromCache = self.isCacheValid()

        print(f"Dados carregados do cache {self.cacheTimestamp}") if self.dataFromCache else self.fetchEveryPokemon()
    
    def isCacheValid(self):
        return self.cacheTimestamp and (time.time() - self.cacheTimestamp < self.cacheDuration)
    
    def fetchData(self, url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erro ao acessar a API: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")

        return None

    def parsePageData(self, data):
        return data.get("results", [])
    
    def fetchEveryPokemon(self):
        run = True

        while run:
            completeUrl = f"{self.baseUrl}?offset={self.offset}&limit={self.limit}"
            completePageData = self.fetchData(completeUrl)
            if completePageData:
                self.allPokemon.extend(self.parsePageData(completePageData))

                if completePageData["next"]:
                    self.offset += self.limit
                else:
                    run = False
            else:
                run = False
        
        saveCache(self.allPokemon, self.cacheDirectory)
        
    def getAllPokemon(self):
        return self.allPokemon
