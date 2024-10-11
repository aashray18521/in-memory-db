from typing import List
import uuid
from enum import Enum
from datetime import datetime


storage_data = {}
class ColumnType(Enum):
  STRING = 'STRING'
  INTEGER = 'INTEGER'


class Column:
  
  def __init__(self, column_name, column_type) -> None:
    self.column_name = column_name
    self.column_type = column_type


class Database:

  def __init__(self, database_name) -> None:
    self.database_name = database_name

  def delete_table(self, table_name):
    try:
      if(storage_data.get(table_name)):
        print(f"[INFO]DELETING TABLE : {table_name}")
        del storage_data[table_name]
        return
    except:
      raise Exception(f"[ERROR]{table_name} does not exist.")


class Table:

  def __init__(self, table_name, column_names: List[Column]) -> None:
    self.table_name = table_name
    self.column_names = column_names

    storage_data[self.table_name] = {
      'table_statistics' : self.column_names,
      'table_data' : [],
      'metadata_info': {
        'created_at' : datetime.now()
      }
    }

  def get_table_information(self):
    for column in self.column_names:
      print(f"Table : {self.table_name} COLUMN", column.__dict__)
  
  def insert_row(self, payload):
    payload['id'] = uuid.uuid1()
    storage_data[self.table_name]['table_data'].append(payload)
    return payload['id']
    
  def get_all_rows(self):
    try:
      # print(f"storage_data : {storage_data}")
      table_data = storage_data.get(self.table_name).get('table_data', []) if storage_data.get(self.table_name) else []
      for data in table_data:
        print(data)
    except:
      raise Exception('[ERROR]Table does not exist in the database.')

  # def delete_row_by_id(self, row_id):

  def filter_records(self, column_name=None, column_value=None, id=None):
    filtered_data = []
    table_data = storage_data.get(self.table_name).get('table_data', [])
    for data in table_data:
      if((id and data['id'] == id) or
         (column_name and column_value and data.get(column_name)) and data[column_name] == column_value):
        filtered_data.append(data)
    return filtered_data

if(__name__=="__main__"):
  # type: ignore 
  # Stuff to do

  db = Database("main_db")
  column_name = Column("name", ColumnType.STRING)
  column_age = Column("age", ColumnType.INTEGER)
  column_salary = Column("salary", ColumnType.INTEGER)

  table_columns = [column_name, column_age, column_salary]
  identity_table = Table("identity_table", table_columns)

  payload_1 = {
    "name": "Ram",
    "age": 52,
    "salary": 3000
  }

  payload_2 = {
    "name": "Shyam",
    "age": 43,
    "salary": 600
  }

  payload_3 = {
    "name": "Karan",
    "age": 65,
    "salary": 70000
  }

  payload_4 = {
    "name": "Lakshmi",
    "age": 32,
    "salary": 100000
  }

  insert1 = identity_table.insert_row(payload=payload_1)
  insert2 = identity_table.insert_row(payload=payload_2)
  insert3 = identity_table.insert_row(payload=payload_3)
  insert4 = identity_table.insert_row(payload=payload_4)

  print("IDENTITY TABLE ", identity_table.get_all_rows())

  print("insert 1 output : ", identity_table.filter_records(id=insert1))
  print("insert 2 output : ", identity_table.filter_records(id=insert2))
  print("insert 3 output : ", identity_table.filter_records(id=insert3))
  print("insert 4 output : ", identity_table.filter_records(id=insert4))

  db.delete_table('identity_table')

  print("DELETE VERIFICATION : ", identity_table.get_all_rows())
