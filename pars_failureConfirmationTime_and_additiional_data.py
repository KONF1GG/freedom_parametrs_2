import contextlib
import datetime
import aiohttp
import asyncio
import clickhouse_connect
import json
import config
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

@contextlib.asynccontextmanager
async def aiohttp_session():
    '''
    Генератор для создания и управления сессией aiohttp
    '''
    async with aiohttp.ClientSession() as session:
        yield session

@contextlib.asynccontextmanager
async def clickhouse_session():
    '''
    Генератор для создания и управления сессией ClickHouse
    '''
    client = clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD
    )
    try:
        yield client
    finally:
        client.close()
        
async def get_all_data_from_date(date: datetime.date):
    '''
    Асинхронно получить все данные начиная с конкретной даты
    '''
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    formatted_date = yesterday.strftime('%Y%m%d')
    url = f'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query=failureConfirmationTime&dt_dt={formatted_date}'
    
    # urls = []
    # current_date = date
    # while current_date <= yesterday:
    #     formatted_date = current_date.strftime('%Y%m%d')
    #     urls.append(f'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Grafana/anydata?query=failureConfirmationTime&dt_dt={formatted_date}')
    #     current_date += datetime.timedelta(days=1)

    raw_data = await fetch_all_data([url]) 
    # raw_data = await fetch_all_data(urls)
    parsed_data = [json.loads(item) for item in raw_data if item] 
    flattened_data = [record for batch in parsed_data for record in batch] 
    await upload_to_clickhouse_failureConfirmationTime(flattened_data) 

async def fetch_all_data(urls):
    '''
    Асинхронно получить данные по списку ссылок с ограничением параллельных запросов
    '''
    semaphore = asyncio.Semaphore(10)  
    async with aiohttp_session() as session:
        tasks = [fetch_data_with_semaphore(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

async def fetch_data_with_semaphore(session, url, semaphore):
    '''
    Асинхронно получить данные по одной ссылке с использованием семафора
    '''
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

async def upload_to_clickhouse_failureConfirmationTime(data):
    '''
    Асинхронно выгрузить данные в ClickHouse в таблицу grafana.failureConfirmationTime
    '''
    async with clickhouse_session() as client:
        rows = [
            (
                item.get('id'),
                item.get('name'),
                item.get('openingDate'),
                item.get('closingDate'),
                item.get('confirmationDate'),
                item.get('importance')
            )
            for item in data
        ]

        if rows:
            column_names = ['id', 'name', 'openingDate', 'closingDate', 'confirmationDate', 'importance']
            client.insert('grafana.failureConfirmationTime', rows, column_names=column_names)
            client.command('OPTIMIZE TABLE grafana.failureConfirmationTime FINAL;')
            logging.info(f"Вставлено {len(rows)} новых записей в grafana.failureConfirmationTime")
        else:
            logging.info("Нет новых записей для вставки в grafana.failureConfirmationTime")

async def get_additional_data():
    '''
    Асинхронно получить дополнительные данные и выгрузить их в ClickHouse
    '''
    url = 'http://server1c.freedom1.ru/UNF_CRM_WS/hs/Userside/site?request=getAdditionData'
    async with aiohttp_session() as session:
        async with session.get(url) as response:
            raw_data = await response.json() 

    current_time = datetime.date.today().strftime('%Y-%m-%d')  
    rows = [
        {'prop': key, 'value': value, 'date': current_time}
        for key, value in raw_data.items()
    ]
    await upload_to_clickhouse_additional_data(rows)

async def upload_to_clickhouse_additional_data(data):
    '''
    Асинхронно выгрузить дополнительные данные в ClickHouse
    '''
    async with clickhouse_session() as client:
        rows = [
            (
                item.get('prop'), 
                item.get('value'), 
                datetime.datetime.strptime(item.get('date'), '%Y-%m-%d').date()
            )
            for item in data
        ]
        column_names = ['prop', 'value', 'date']
        client.insert('grafana.indicators', rows, column_names=column_names)

async def main():
    '''
    Главная функция
    '''
    logging.info("Скрипт начал выполнение")
    try:
        start_date = datetime.date(2025, 3, 1)
        await get_all_data_from_date(start_date)
        await get_additional_data()
        logging.info("Скрипт успешно завершил выполнение")
    except Exception as e:
        logging.error(f"Ошибка во время выполнения скрипта: {e}")

        raise

if __name__ == "__main__":
    asyncio.run(main())