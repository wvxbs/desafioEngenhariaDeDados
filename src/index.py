from pokemon import Pokemon

def printData(data):
    for i in data:
        print(i)

def main():
    i = 0
    while i in range(3):
        base_url = "https://pokeapi.co/api/v2/pokemon"
        p = Pokemon(base_url, "/app/cache", 3600)

        printData(p.getAllPokemon())
        i += 1
main()