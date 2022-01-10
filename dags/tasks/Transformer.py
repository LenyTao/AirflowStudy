import pandas as pd


# Операция трансформации данных
def transform_data():
    pd.set_option('display.max_columns', None)

    # Считываем наш csv файл в DataFrame

    df = pd.read_csv("input.csv", delimiter=';', header=None)

    # Задаём для него схему
    df.columns = [
        'sign_date',
        'blank',
        'creation_date',
        'name',
        'index',
        'type_of_organization',
        'sector',
        'payout type',
        'amount',
        'period_of_date',
        'blank2'
    ]

    # Избавляемся от ненужных колонок:
    df.drop(["blank"], axis=1, inplace=True)
    df.drop(["blank2"], axis=1, inplace=True)

    # Очищаем DataFrame от пустых строк, за основу берётся поле, в котором всегда есть значение
    df = df.dropna(subset=['sign_date'])

    # Трансформация 1: "1 от 17.09.2018" -> 17.09.2018

    df['sign_date'] = df['sign_date'] \
        .str.extract(r'от (.*)', expand=False) \
        .str.extract(r'(\d\d.\d\d.\d\d)', expand=False)

    # Трансформация 2: "ООО ""Континент ЭТС""" -> "ООО" | "Континент ЭТС"

    df['legal_form'] = df['name'].str.split(' ').str[0].str.strip()
    df['organization_name'] = df['name'].str.extract(r' (.*)', expand=False).str.replace('\"', '').str.strip()
    df.drop(["name"], axis=1, inplace=True)
    #
    # Трансформация 3: “1 014 430,57” -> 1014430,57

    df['amount'] = df['amount'].str.replace(",", ".").str.replace(" ", "")

    # Трансформация 4: 2018-2020 -> 2018 | 2020

    df['beginning_contract'] = df['period_of_date'].str.strip().str[0:4]
    df['ending_contract'] = df['period_of_date'].str.strip().str[-4:]

    df.drop(["period_of_date"], axis=1, inplace=True)

    # Приводим данные из некоторых столбцов к необходимым типам
    df['index'] = df['index'].astype('int64')
    df['amount'] = df['amount'].astype('float64')

    # # Записываем готовый фрейм в файл
    df.to_csv("./output.csv", index=False, sep=';')
