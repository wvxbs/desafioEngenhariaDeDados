from pokemon import Pokemon

def main():
    base_url = "https://pokeapi.co/api/v2/pokemon"
    Pokemon(base_url, "/app/cache", 3600)

main()