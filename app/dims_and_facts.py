from collections import namedtuple
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

# List of all tables used in the original database
TABLES = [
    "addresses",
    "birthdates",
    "cities",
    "countries",
    "cuisines",
    "districts",
    "food",
    "orders",
    "promos",
    "restaurants",
    "states",
    "users",
]

# Path to the directory where tables' CSV files are stored
TABLES_DIR_PATH = Path(__file__).parent / "tables"

# Structure holding initial database
MultiDimDatabase = namedtuple(
    "MultiDimDatabase",
    [
        "addresses",
        "birthdates",
        "cities",
        "countries",
        "cuisines",
        "districts",
        "food",
        "orders",
        "promos",
        "restaurants",
        "states",
        "users",
    ],
)
ReducedDatabase = namedtuple(
    "ReducedDatabase", ["orders", "users", "food",
                        "promos", "restaurants", "addresses"]
)


# --- Task #1 ---

def load_tables(tables_dir_path: Path, tables: List[str]) -> List[pd.DataFrame]:
    load_tables = []
    for table in tables:
        table_path = tables_dir_path/f"{table}.csv"
        df = pd.read_csv(table_path, index_col=0)
        load_tables.append(df)
        # if table_path == tables_dir_path/"addresses.csv":
        #     load_tables.append(df[["district_id","street"]])
        # else:
        #     load_tables.append(df)
    # print(load_tables)
    return load_tables

# --- Task # 2 ---


def reduce_dims(db: MultiDimDatabase) -> ReducedDatabase:
    reduced_db = []
    address_df = pd.DataFrame(db.addresses)
    address_df = address_df.merge(
        db.districts, left_on='district_id', right_index=True, how='left')
    address_df = address_df.merge(
        db.cities, left_on='city_id', right_index=True, how='left')
    address_df = address_df.rename(
        columns={'name_x': 'district', 'name_y': 'city'})
    address_df = address_df.merge(
        db.states, left_on='state_id', right_index=True, how='left')
    address_df = address_df.merge(
        db.countries, left_on='country_id', right_index=True, how='left')
    address_df = address_df.rename(
        columns={'name_x': 'state', 'name_y': 'country'})
    address_df.drop(['district_id', 'city_id', 'state_id',
                    'country_id'], axis=1, inplace=True)
    # print("ADDRESS",address_df.dtypes)
    address_df = address_df.astype(
        {'city': object, 'country': object, 'district': object, 'state': object, 'street': object})
    # print("ADDRESS",address_df.dtypes)

    food_df = pd.DataFrame(db.food)
    food_df = food_df.rename(columns={'cuisine_id': 'cuisine'})
    # print("FOOD",food_df.dtypes)
    food_df = food_df.astype(
        {'name': object, 'cuisine': object, 'price': float})
    # print("FOOD",food_df.dtypes)

    orders_df = pd.DataFrame(db.orders)
    # print("FOOD",orders_df.dtypes)
    orders_df['ordered_at'] = pd.to_datetime(orders_df['ordered_at'])
    # print("FOOD",orders_df.dtypes)

    birthdates_df = pd.DataFrame(db.birthdates)
    birthdates_df.index.name = 'birthdate'
    print("BIRTH", birthdates_df.dtypes)
    # birthdates_df = birthdates_df.astype({'first_name': object, 'last_name': object})

    users_df = pd.DataFrame(db.users)
    users_df = users_df.rename(columns={'birthdate_id': 'birthdate'})
    # print("USER",users_df.dtypes)
    users_df = users_df.astype({'first_name': object, 'last_name': object})
    users_df['birthdate'] = pd.to_datetime(
        users_df['birthdate'], format='%d/%m/%Y')
    users_df['registred_at'] = pd.to_datetime(users_df['registred_at'])
    # print("USER",users_df.dtypes)

    columns_list = ['country', 'state', 'city', 'district', 'street']
    address_df = address_df.reindex(columns=columns_list)
    reduced_db.append(address_df)

    for table in db[1:]:
        if table.index.name == 'food_id':
            reduced_db.append(food_df)
        elif table.index.name == 'birthdate_id':
            reduced_db.append(birthdates_df)
        elif table.index.name == 'user_id':
            reduced_db.append(users_df)
        else:
            reduced_db.append(table)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # print(reduced_db)

    return ReducedDatabase(orders=reduced_db[7], users=reduced_db[11], food=reduced_db[6], promos=reduced_db[8], restaurants=reduced_db[9], addresses=reduced_db[0])

# --- Task #3 ---


def create_orders_by_meal_type_age_cuisine_table(db: ReducedDatabase) -> pd.DataFrame:
    create_df = db.orders
    create_df = create_df.merge(
        db.food, left_on='food_id', right_index=True, how='left')
    create_df = create_df.merge(
        db.users, left_on='user_id', right_index=True, how='left')
    # print(create_df['ordered_at'])

    create_df['ordered_at'] = pd.to_datetime(create_df['ordered_at'])
    create_df['meal_type'] = 'dinner'
    create_df.loc[(create_df['ordered_at'].dt.hour >= 6) & (
        create_df['ordered_at'].dt.hour < 10), 'meal_type'] = 'breakfast'
    create_df.loc[(create_df['ordered_at'].dt.hour >= 10) & (
        create_df['ordered_at'].dt.hour <= 16), 'meal_type'] = 'lunch'
    # print(create_df['meal_type'])

    create_df['birthdate'] = pd.to_datetime(
        create_df['birthdate'], format='%d/%m/%Y')
    create_df['user_age'] = 'old'
    create_df.loc[(create_df['birthdate'].dt.year >= 1995),
                  'user_age'] = 'young'
    create_df.loc[(create_df['birthdate'].dt.year >= 1970) & (
        create_df['birthdate'].dt.year < 1995), 'user_age'] = 'adult'
    # print(create_df['user_age'])

    create_df = create_df.rename(columns={'cuisine': 'food_cuisine'})

    create_df.drop(['user_id', 'address_id', 'restaurant_id', 'food_id', 'ordered_at', 'promo_id',
                   'name', 'price', 'first_name', 'last_name', 'birthdate', 'registred_at'], axis=1, inplace=True)

    create_df = create_df.astype({'food_cuisine': object})

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # print(create_df.dtypes)
    return create_df
