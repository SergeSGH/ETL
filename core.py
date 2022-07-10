import requests
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import time
import datetime
# for webhook


def get_URI(query: str, page_num: str, date: str, API_KEY: str) -> str:
    """# возвращет URL к статьям для текущего запроса по номеру страницы и дате """

    # добавляем запрос к uri
    URI = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}'

    # добавляем номер страницы и дату
    URI = URI + f'&page={page_num}&begin_date={date}&end_date={date}'

    # добавляем ключ API
    URI = URI + f'&api-key={API_KEY}'

    return URI


df = pd.DataFrame()

current_date = datetime.datetime.now().strftime('%Y%m%d')

page_num = 1

while True:

    URI = get_URI(query='COVID', page_num=str(page_num), date=current_date, API_KEY=API_KEY)

    # делаем запрос по URI
    response = requests.get(URI)
    # преобразуем результат в формат JSON
    data = response.json()

    # преобразуем данные в фрейм данных
    df_request = json_normalize(data['response'], record_path=['docs'])

    # прерываем цикл если отсутсвуют новые записи
    if df_request.empty:
        break

    # добавляем записи в конец дата фрейма
    df = pd.concat([df, df_request])

    # пауза для требования по количеству запросов
    time.sleep(6)

    # переходим на следующую страницу
    page_num += 1

# ищем дубликаты и удаляем их
if len(df['_id'].unique()) < len(df):
    print('There are duplicates in the data')
    df = df.drop_duplicates('_id', keep='first')

# ищем и удаляем записи без заголовков
if df['headline.main'].isnull().any():
    print('There are missing values in this dataset')
    df = df[not df['headlinee.main'].isnull()]

# фильтруем op-ed статьи
df = df[df['type_of_material']!='op-ed']

# оставляем только поля headline, publication_date, author name и url
df = df[['headline.main', 'pub_date', 'byline.original', 'web_url']]

# переименовываем колонки columns
df.columns = ['headline', 'date', 'author', 'url']

# создаем объект engine для БД
database_loc = f"postgresql://{username}:{password}@localhost:5432/{database}"
engine = create_engine(database_loc)

df_test.to_sql(name='news_articles', con=engine, index=False,
               if_exists='append')
