from config import host, user, passw, database, port, UserAgents

import pysnooper

import requests
import asyncio
import aiohttp
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, insert, text, Column, Table, MetaData, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

import json

import math

from time import sleep

import threading

from datetime import datetime

days_max = 14
offer_max = 6
artics_for_proxy_max=6
market_id = 31655
proxy_bug_list = []
list_for_data_to_sql = []
threads_count = 0


engine = create_engine(
    'mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database,
    pool_size=999,
    echo=False,
    pool_pre_ping=True)
Base = declarative_base()
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

class Result(Base):  # создает копию таблицы. нужно для загрузки инфы в дб в конце парса артикла
    __tablename__ = "result_8"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    id_1c_part = Column("id_1c_part", String)
    id_1c_doc = Column("id_1c_doc", String)
    part_sought = Column("part_sought", String)
    brand_sought = Column("brand_sought", String)
    part_result = Column("part_result", String)
    brand_result = Column("brand_result", String)
    title = Column("title", String)
    price = Column("price", Integer)
    day = Column("day", Integer)
    qty = Column("qty", String)
    supplier = Column("supplier", String)
    rating = Column("rating", String)
    good_percent = Column("good_percent", String)
    bad_percent = Column("bad_percent", String)
    location = Column("location", String)
    source = Column("source", String)


async def start(artics_list, proxy_list):
    tasks = []
    for index, articel in enumerate(artics_list):
        proxy_index = index // artics_for_proxy_max
        proxy = proxy_list[proxy_index][0]
        tasks.append(get_articel(articel[0], articel[1], proxy))
    await asyncio.gather(*tasks)

    for proxy in proxy_list:
        sql_message(f"UPDATE proxy SET status = 1 WHERE proxy = '{proxy[0]}'")
    for proxy_bug in proxy_bug_list:
        sql_message(f"UPDATE proxy SET status = 2 WHERE proxy = '{proxy_bug}'")

async def get_articel(articel, brand, proxy):

    global list_for_data_to_sql, proxy_bug_list
    connector = ProxyConnector.from_url(f'socks5://{proxy}')

    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            headers={'Referer':'https://emex.ru/'}
            async with session.get(f'https://emex.ru/api/search/search?make={brand}'
                                           f'&detailNum={articel}&locationId={market_id}'
                                           f'&showAll=true&longitude=37.5381&latitude=55.8781', headers=headers,
                                           timeout=20) as response:

                cookies = {}
                for cookie_str in response.headers.getall('Set-Cookie'):
                    key, value = cookie_str.split(';')[0].split('=')
                    cookies[key] = value

                json_response = await response.json()
                filtered_products_by_time = [product for product in json_response['searchResult']['originals'][0]['offers'] if product['delivery']['value']<=days_max]
                sorted_products_by_price = sorted(filtered_products_by_time, key=lambda x:x['price']['value'])
                top_6_products = sorted_products_by_price[:offer_max]

                offers = []
                for offer in top_6_products:
                    name = json_response['searchResult']['name']
                    delivery = offer['delivery']['value']
                    price = offer['price']['value']
                    quantity = offer['quantity']
                    pack_returned = await get_offer_priceLogo(offer['offerKey'],cookies)
                    one_offer = [name,articel,brand,delivery,price,quantity,pack_returned]
                    offers.append(one_offer)
                print('Добавил в список на добавление')
                list_for_data_to_sql.append([offers, articel])
        except Exception as ex:
            proxy_bug_list.append(proxy)
            print(f'Не спарсил {articel} {brand}')
            print(ex)
            print(proxy)
            print('\n')
        finally:
            await connector.close()

async def get_offer_priceLogo(offerkey,cookies):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                it = await session.get(f'https://emex.ru/api/search/rating?offerKey={offerkey}]', cookies=cookies, timeout=5)
                async with it as response:
                    json_response = await response.json()

                    priceLogo = json_response['priceLogo']
                    try:
                        value = json_response['value']
                    except:
                        value = 0
                    goodPercent = json_response['goodPercent']
                    badPercent = json_response['badPercent']
                    pack_to_return = [priceLogo,value,goodPercent,badPercent]
                    return pack_to_return
            except asyncio.TimeoutError:
                print('asyncio.TimeoutError в предложении')
            except Exception as ex:
                print(ex)
                print('Проблема в предложении')

def data_to_sql():

    global list_for_data_to_sql, threads_count
    count = 0
    while True:
            if len(list_for_data_to_sql) != 0:

                list_in_thread= list_for_data_to_sql.copy()
                try:
                    list_for_data_to_sql.pop(0)
                    offers = list_in_thread[0][0]
                    articel = list_in_thread[0][1]
                    list_for_sql = []

                    try:
                        session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
                    except:
                        engine.connect()
                        session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
                    for one_offer in offers:
                        id_1c_part = sql_message(f"SELECT `id_1c_part` FROM `needs_8` WHERE part_sought='{articel}'").first()[0]
                        id_1c_doc = sql_message(f"SELECT `id_1c_doc` FROM `needs_8` WHERE part_sought='{articel}'").first()[0]
                        articel = one_offer[1]
                        brand_sought = one_offer[2]
                        name = one_offer[0]
                        price = one_offer[4]
                        delivery = one_offer[3]
                        quantity = one_offer[5]
                        supplier = one_offer[6][0]
                        supplier_rating = one_offer[6][1]
                        good_percent = one_offer[6][2]
                        bad_percent = one_offer[6][3]
                        location = 'Москва'
                        source = 'Original'




                        list_for_sql.append(Result(id_1c_part=id_1c_part, id_1c_doc=id_1c_doc, part_sought=articel,
                                               brand_sought=brand_sought, part_result=articel,
                                               brand_result=brand_sought,
                                               title=name, price=price, day=delivery, qty=quantity, supplier=supplier,
                                               rating=supplier_rating, good_percent=good_percent,
                                               bad_percent=bad_percent,
                                               location=location, source=source))
                    session.execute(f"UPDATE `needs_8` SET status=0 WHERE part_sought='{articel}'")

                    session.add_all(list_for_sql)
                    session.commit()
                    session.close()
                    count += 1

                    print(f'Еще один уже в ДБ! {count}')
                except Exception as ex:
                    print(list_in_thread)
                    print(ex)
            else:
                print('Конец потока загрузки в браузер')

                break
    threads_count -= 1

def proxy_checker():
    sql_message("UPDATE `proxy` SET `status` = 0 WHERE `status` = 1 OR 2 OR 3")
    proxies = sql_message("SELECT proxy FROM proxy WHERE status = 0").fetchall()

    async def check_proxy(proxy):
        url = 'https://thispersondoesnotexist.com/'
        connector = ProxyConnector.from_url(f'socks5://{proxy}')

        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                start_time = time.time()
                async with session.get(url, timeout=30) as response:
                    elapsed_time = time.time() - start_time
                    if response.status == 200:
                        print(elapsed_time)
                        sql_message(f"UPDATE `proxy` SET status=1 WHERE proxy='{proxy}'")
                        print(f'Proxy {proxy} is working')

                    else:
                        sql_message(f"UPDATE `proxy` SET status=2 WHERE proxy='{proxy}'")
                        print(f'Proxy {proxy} is not working, response code: {response.status}')
            except asyncio.TimeoutError:
                sql_message(f"UPDATE `proxy` SET status=2 WHERE proxy='{proxy}'")
                print(f'Proxy {proxy} timed out')
            except aiohttp.ClientError as e:
                sql_message(f"UPDATE `proxy` SET status=2 WHERE proxy='{proxy}'")
                print(f'Proxy {proxy} is not working: {e}')
            except Exception as ex:
                print(f'An error occurred with proxy {proxy}: {ex}')  # Вывод ошибки для отладки
                sql_message(f"UPDATE `proxy` SET status=2 WHERE proxy='{proxy}'")
            finally:
                await connector.close()

    async def main():
        tasks = [check_proxy(proxy[0]) for proxy in proxies]
        try:
            await asyncio.gather(*tasks)
        except Exception as ex:
            print(ex)
            pass

        session.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


def sql_message(message):
    try:
        session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=engine))
    except:
        engine.connect()
        session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=engine))
    response = session.execute(message)
    session.close()
    return response

def get_now():
    return str(datetime.now()).split('.')[0]

def status_check():
    global threads_count
    while True:
        threads_list = []

        try:
            engine.dispose()
        except:
            pass

        while True:
            stop_status = sql_message("SELECT STATUS FROM django_config WHERE ID = 3").first()[0]
            if not bool(stop_status):
                break
            else:
                sleep(5)

        artics_list = sql_message("SELECT part_sought, brand_sought FROM needs_8 WHERE status = 1").fetchall()
        if len(artics_list) == 0:
            sql_message("UPDATE django_config SET status = 1 WHERE ID = 3")
            sql_message("UPDATE django_config SET status = 0 WHERE ID = 1")
            sql_message(f"INSERT INTO `log` (`text`) VALUES ('{get_now()}: Артиклей для парса нет. Парсинг закончен')")

        if bool(sql_message("SELECT STATUS FROM django_config WHERE ID = 1").first()[0]):
            gotovo = sql_message('SELECT COUNT(*) FROM needs_8 WHERE status = 0').first()[0]

            sql_message(f"INSERT INTO `log` (`text`) VALUES ('{get_now()}: На данный момент готово {gotovo}')")

            artics_list = sql_message(
                "SELECT part_sought, brand_sought FROM needs_8 WHERE status = 1 LIMIT 60").fetchall()
            number_of_proxy = math.ceil(
                len(artics_list) / artics_for_proxy_max)  # всегда округляет в большую сторону после деления.
            proxy_list = sql_message(f"SELECT proxy FROM proxy WHERE status = 1 LIMIT {number_of_proxy}").fetchall()

            for proxy in proxy_list:
                sql_message(f"UPDATE proxy SET status = 3 WHERE proxy = '{proxy[0]}'")
            if len(proxy_list) < number_of_proxy:
                proxy_checker()
                continue

            loop = asyncio.get_event_loop()
            loop.run_until_complete(start(artics_list, proxy_list))
            print(f'{len(list_for_data_to_sql)} колво артиклей')
            for _ in range(4):
                thread = threading.Thread(target=data_to_sql)
                thread.start()
                threads_list.append(thread)
                threads_count += 1
            while True:
                sleep(1)
                if threads_count == 0:
                    for thread in threads_list:
                        thread.join()
                    break
status_check()
