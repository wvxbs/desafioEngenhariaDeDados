import os
import json
import time

def loadCache(cacheDirectory):
    latestCache = None
    latestTimestamp = 0
    cache = {}
    
    for filename in os.listdir(cacheDirectory):
        if filename.endswith('.json'):
            try:
                timestamp = int(filename.split('_')[1].split('.')[0])
                if timestamp > latestTimestamp:
                    latestTimestamp = timestamp
                    latestCache = os.path.join(cacheDirectory, filename)
            except ValueError:
                print(f"Erro ao extrair timestamp do arquivo: {filename}")
    
    if latestCache:
        try:
            with open(latestCache, 'r') as f:
                cache = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Erro ao carregar o cache: {e}")
    
    return cache, latestTimestamp

def saveCache(allPokemon, cacheDirectory):
    timestamp = int(time.time())
    cacheFilename = f"cache_{timestamp}.json"
    cacheFilepath = os.path.join(cacheDirectory, cacheFilename)
    try:
        with open(cacheFilepath, 'w') as f:
            json.dump({'data': allPokemon, 'timestamp': timestamp}, f)
            print(f"Cache salvo no arquivo: {cacheFilepath}")
    except IOError as e:
        print(f"Erro ao salvar o cache: {e}")
