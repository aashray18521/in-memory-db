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
  
  # def insert_row(self, payload):
    
  # def get_all_rows(self):

  # def delete_row_by_id(self, row_id):

  # def filter_records(self, column_name=None, column_value=None, id=None):

if(__name__=="__main__"):
  # Stuff to do
