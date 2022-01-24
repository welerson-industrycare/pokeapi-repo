import os
import sys
import pika
import json
import psycopg2

#List of pokemon evolutions
evolutions = []

#Connect to Postgres
def connect():
    """
    Connection to PostgresSQL
    @return conn: Connection 
    """
    conn = None

    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    db_pwd = os.environ['DB_PASSWORD']

    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pwd 
        )
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            return conn

def insert_pokemons(pokemon_list):
    """
    Insert the pokemon stats into the pokemons table
    @param pokemon_list: List of pokemon stats
    """
    conn = None
    try:
        conn = connect()
        sql = """
            INSERT INTO pokemons(name, height, base_experience, is_default, hp, attack, defense, special_attack, special_defense, speed)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(sql, pokemon_list)
            conn.commit()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_evolutions():
    """
    Insert the pokemon chain evolution into the evolutions table
    """
    conn = None
    try:
        conn = connect()
        sql = """
            INSERT INTO evolutions(first_form, second_form, third_form)
                VALUES(%s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(sql, evolutions)
            conn.commit()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_pokemon_types(pokemon_types):
    """
    Insert the pokemon types relation into the pokemon_types table
    @param pokemon_types: List of relations between pokemons and types
    """
    conn = None
    try:
        conn = connect()
        sql = """
            INSERT INTO pokemon_types(pokemon_id, type_id)
                VALUES(%s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(sql, pokemon_types)
            conn.commit()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_type(type_names):
    """
    Insert the type names into the types table
    @param type_names: List of pokemon type names
    """
    conn = None
    try:
        conn = connect()
        sql = """
            INSERT INTO types(name)
                VALUES(%(name)s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(sql, type_names)
            conn.commit()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def pokemon_handler(pokemon_list):
    """
    Handle the pokemon endpoint data
    @param pokemon_list: List of pokemon stats
    """
    pokemon_stats = get_pokemon(pokemon_list)
    insert_pokemons(pokemon_stats)

def evolution_handler(evolution_list):
    """
    Handle the evolution chain endpoint data
    @param evolution_list: List of evolution chain
    """
    get_evolution_chain(evolution_list)
    insert_evolutions()

def type_handler(type_list):
    """
    Handle the types endpoint data
    @param type_list: List of pokemon types
    """    
    #Types
    types_names, types_list = get_type(type_list)
    insert_type(types_names)

    #Type relations
    pokemon_types = set_relation_type(types_list)
    insert_pokemon_types(pokemon_types)

def get_pokemon(list_pokemons):
    """
    Get some pokemon stats
    @param list_pokemons: List of pokemon stats
    @return pokemon_list: List of sets containing pokemon stats
    """
    pokemon_list = []
    for l in list_pokemons:
        name = l['name']
        height = l['height']
        base_experience = l['base_experience']
        is_default = l['is_default']
        stats = l['stats']
        hp = stats[0]['base_stat']
        attack = stats[1]['base_stat']
        defense = stats[2]['base_stat']
        special_attack = stats[3]['base_stat']
        special_defense = stats[4]['base_stat']
        speed = stats[5]['base_stat']
        
        pokemon_list.append((name, height, base_experience, is_default,
        hp, attack, defense, special_attack, special_defense, speed))
        
    return pokemon_list

def get_evolution_chain(list_evolution):
    """
    Get  pokemon evolution chain
    @param list_evolution: List of evolution chain
    """
    for l in list_evolution:
        tmp_list = []
        level = 0
        chain = l['chain']
        parent = chain['species']['name']
        tmp_list.append(parent)

        get_evolution_tree(chain, tmp_list, level)
        

def get_evolution_tree(chain, chain_list, level):
    """
    Append pokemon chain_list in evolutions
    @param chain: Dict with evolution chain
    @param chain_list: List with pokemon name
    @param level: Level of pokemon in evolution chain
    """
    if chain['evolves_to']:
        for i in chain['evolves_to']:
            parent = i['species']['name']
            if level == 0:
                chain_list = chain_list[:1]
            chain_list.append(parent)
            evo = get_evolution_tree(i, chain_list, level+1)

            if evo:
                if len(evo) == 2:
                    evo.append(None)
                    evolutions.append(evo.copy())
                    chain_list = chain_list[:-2]
                else:
                    evolutions.append(evo.copy())
                    chain_list = chain_list[:-1]                    
                
    else:
        return chain_list

def get_type(list_types):
    """
    Get all types
    @param list_types: List of pokemon types
    @return type_names: List of type names
    @return type_list: List of sets containing pokemon name and type
    """
    type_list = []
    type_names = []

    for l in list_types:
        type = l['name']
        type_names.append({
            'name':type
        })
        pokemon_list = l['pokemon']
        for p in pokemon_list:
           pokemon = p['pokemon']['name']
           type_list.append((
               pokemon, type
           )) 

    return type_names, type_list


def get_all_types():
    """
    Get all types registered in the database
    @return data: Dict with type name as key and type_id as value
    """
    data = -1
    conn = None
    try:
        conn = connect()
        sql = """
            SELECT 
                name,
                type_id
            FROM types
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dict(cursor.fetchall())
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return data


def get_all_pokemons():
    """
    Get all pokemons registered in the database
    @return data: Dict with pokemon name as key and pokemon_id as value
    """
    data = -1
    conn = None
    try:
        conn = connect()
        sql = """
            SELECT 
                name,
                pokemon_id
            FROM pokemons
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dict(cursor.fetchall())
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return data

def set_relation_type(type_list):
    """
    Set the ralation between pokemon and type
    @param type_list: List of sets containing pokemon name and type name
    @param pokemon_types: List of sets containing pokemon_id and type_id 
    """
    pokemon_types = []
    types = get_all_types()
    pokemons = get_all_pokemons()

    for t in type_list:
        pokemon_id = pokemons[t[0]]
        type_id = types[t[1]]
        pokemon_types.append((pokemon_id, type_id))

    return pokemon_types

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline')

    def callback(ch, method, properties, body):
        data = json.loads(body)
        if 'pokemon' in data.keys():
            pokemon_handler(data['pokemon'])
        elif 'evolution' in data.keys():
            evolution_handler(data['evolution'])
        else:
            type_handler(data['types'])


    channel.basic_consume(queue='pipeline', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)