import pika
import json
import requests

def get_pokemon_url():
    """
    Get all pokemons urls
    @return url_list: List of urls
    """
    url = 'https://pokeapi.co/api/v2/pokemon?offset=0&limit=3000'
    r =  requests.get(url)
    res = json.loads(r.text)
    url_list = res['results']

    return url_list

def get_type_url():
    """
    Get all types urls
    @return url_list: List of urls
    """
    url = 'https://pokeapi.co/api/v2/type'
    r =  requests.get(url)
    res = json.loads(r.text)
    url_list = res['results']

    return url_list

def get_evolution_url():
    """
    Get all evolution-chain urls
    @return url_list: List of urls
    """
    url = 'https://pokeapi.co/api/v2/evolution-chain?offset=0&limit=500'
    r = requests.get(url)
    res = json.loads(r.text)
    url_list = res['results']

    return url_list

def get_pokemon(pokemon_url):
    """
    Get some pokemon stats
    @param url_list: List of urls
    @return pokemon_list: List of jsons containing pokemon stats
    """
    pokemon_list = []
    for p in pokemon_url:
        url = p['url']
        res =  requests.get(url)
        data = json.loads(res.text)
        pokemon_list.append(data)
        
    return pokemon_list

def get_evolution_chain(evolution_url):
    """
    Get  pokemon evolution chain
    @param evolution_url: List of urls
    @return evolution_list: List of jsons evolution chain
    """
    evolution_list = []
    for e in evolution_url:
        url = e['url']
        res = requests.get(url)
        data = json.loads(res.text)
        evolution_list.append(data)
 
    return evolution_list

def get_types(url_list):
    """
    Get all types
    @param url_list: List of type urls
    @return type_list: List of jsons containing pokemon type
    """
    type_list = []
    for u in url_list:
        url = u['url']
        res =  requests.get(url)
        data = json.loads(res.text)
        type_list.append(data)

    return type_list


def send_data(data):
    """
    Send json data
    @param data: Dict containing list of data
    """
    data = json.dumps(data)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='pipeline')
    channel.basic_publish(exchange='', routing_key='pipeline', body=data)
    connection.close()

if __name__ == '__main__':
    
    #Pokemon endpoint
    pokemon_list = get_pokemon_url()
    pokemon_stats = get_pokemon(pokemon_list)
    send_data({'pokemon':pokemon_stats})

    #Evolution Chains endpoint
    evolution_list = get_evolution_url()
    evolution_chain = get_evolution_chain(evolution_list)
    send_data({'evolution':evolution_chain})

    #Types endpoint
    type_list = get_type_url()
    types = get_types(type_list)
    send_data({'types':types})



