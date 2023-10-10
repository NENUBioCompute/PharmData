"""
  -*- encoding: utf-8 -*-
  @Author: zhuliujinxiang
  @Time  : 2023/10/10 22:42
  @Email: deepwind32@163.com
  @function
"""
import configparser
import logging
import time
import pymongo
from collections import deque
class BindingbtoMongo:
    """
    The DBConnection is a class for data store, now only insert function supported.

    Parameters:
        - db_name (str): The name of MongoDB database
        - db_url (str, optional): Database url, the default value is "127.0.0.1".
        - port (int, optional): Database port, the default value is 27017
        - db_check (bool, optional): Determine whether to clear the corresponding collection in the database before inserting data. The default is False.
        - user_name(str, optional): MongoDB username. This parameter is optional. By default, the username and password mode are not used
        - password(str, optional): MongoDB password

    Methods:
        - assure_empty(): Empty the specified collection in the database. If db_check is set to True, this method is called before the data is automatically inserted.
        - insert(data: iter, buffer_size=10000, accelerate=False, counter=False): Inserts data from the specified csv file into the corresponding collection.
        - clear_collection(): Clear collection
    """
    def __init__(self, db_name: str, collection_name: str, host: str = "127.0.0.1", port: int = 27017,
                 username: str = None, password: str = None, db_check=True):
        self.client = pymongo.MongoClient(host, port) \
            if username is None and password is None \
            else pymongo.MongoClient(host, port, username=username, password=password)
        self.db = self.client[db_name]
        self.collection_name = collection_name
        self.collection = self.db[collection_name]
        self.assure_empty(db_check)

    def insert(self, data: iter, buffer_size=10000, accelerate=False, counter=False) -> None:
        """
            insert a well-organized dict, so generator is recommended.
            :param data: iterable data
            :param buffer_size: The size of the buffer when data is inserted. The default is 10000
            :param accelerate: enable multi-thread insert. Note that the insert action is unordered.
            :param counter: enable data count when is set True
        """
        logging.info(f"Collection {self.collection_name} start conversion.")
        beginning_time = time.time()
        data_counter = 0

        buffer = deque()
        for item in data:
            buffer.append(item)
            if len(buffer) >= buffer_size:
                requests = [pymongo.InsertOne(doc) for doc in buffer]
                self.collection.bulk_write(requests, ordered=accelerate)
                if counter:
                    data_counter += buffer_size
                buffer.clear()
        # Ensure that all data in the buffer is retrieved
        if buffer:
            requests = [pymongo.InsertOne(doc) for doc in buffer]
            self.collection.bulk_write(requests, ordered=accelerate)
            if counter:
                data_counter += len(buffer)
            buffer.clear()
        if counter:
            logging.info(
                f"Collection {self.collection_name} conversion complete，Insert {data_counter} bar documents, time elapsed {time.time() - beginning_time:.2f}s")
        else:
            logging.info(
                f"Collection {self.collection_name} conversion complete, time elapsed {time.time() - beginning_time:.2f}s")

    def assure_empty(self, dbCheck: bool) -> None:
        # If dbCheck is set to True, this method is called before the data is automatically inserted.
        if dbCheck:
            if self.collection.find_one({}) is None:
                logging.info("Collection is empty!")
            else:
                logging.error("Collection not empty.")
                raise Exception("Collection not empty Error!")

    def clear_collection(self):
        self.collection.drop()


if __name__ == "__main__":
    # The following is a usage example
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    # disable collection empty check
    drugDB = BindingbtoMongo("drugdb", "offsides", db_check=False)