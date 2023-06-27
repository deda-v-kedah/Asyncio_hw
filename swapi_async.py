import re
import asyncio    
import aiohttp
from datetime import datetime
from more_itertools import chunked   
from models import engine, Session, Base, Superheroes



async def get_id(my_url):
    result = re.findall(r'\d+', my_url)
    return int(result[0])

async def paste_to_db(person_json):
    async with Session() as session:
        id = await get_id(person_json['url'])
        print( f'add {id}' )
        orm_objects = [Superheroes( id = id,
                                birth_year = person_json['birth_year'],
                                eye_color = person_json['eye_color'],
                                films = ', '.join(person_json['films']),
                                gender = person_json['gender'], 
                                hair_color = person_json['hair_color'],
                                height = person_json['height'],
                                homeworld  = person_json['homeworld'],   
                                mass = person_json['mass'],
                                name = person_json['name'],
                                skin_color = person_json['skin_color'],
                                species = ', '.join(person_json['species']),
                                starships = ', '.join(person_json['starships']),
                                vehicles = ', '.join(person_json['vehicles']),
                                )]

        session.add_all(orm_objects)
        await session.commit()

    


async def get_count():
    session = aiohttp.ClientSession()
    print(f'<-- COUNT')
    response = await session.get(f'https://swapi.dev/api/people/')
    json_data = await response.json()
    await session.close()
    persons = json_data['results']
    for person in persons:
        if "url" in person:
                asyncio.create_task(paste_to_db(person))
        else:
            print('dont swapi people')
    count = json_data['count']
    print(f'--> count: {count}')
    return  count




async def get_people(people_id):
    session = aiohttp.ClientSession()
    print(f'<-- {people_id}')
    response = await session.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()
    await session.close()
    print(f'--> {people_id}')
    return json_data



async def my_loop(min, max):
    print('loop go')
    person_coros = (get_people(i) for i in range(min, max))
    person_coros_chunked = chunked(person_coros, 5)
    for person_coros_chunk in person_coros_chunked:
        persons = await asyncio.gather(*person_coros_chunk)
        for person in persons:
            if "url" in person:
                asyncio.create_task(paste_to_db(person))

            else:
                print('dont swapi people')

              




async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    task_1 = asyncio.create_task(get_count())  
    task_2 = asyncio.create_task(my_loop(11, 21))
    

    await task_1
    max_id = task_1.result()

    await task_2
    await my_loop(21, max_id+2) 

    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    await asyncio.gather(*tasks)

    


if __name__ == '__main__':
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)
