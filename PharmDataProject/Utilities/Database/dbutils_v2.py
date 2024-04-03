"""
  -*- encoding: utf-8 -*-
  @Author: zhuliujinxiang
  @Time  : 2023/10/00 20:49
  @Email: deepwind32@163.com
  @function 
"""
import configparser
import logging
import time
import pymongo
from collections import deque

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class DBConnection:
    """
    The DBConnection is a class for data store, now only insert function supported.

    Parameters:
        - db_name (str): The name of MongoDB database
        - db_url (str, optional): Database url, the default value is "127.0.0.1".
        - port (int, optional): Database port, the default value is 27017
        - db_check (bool, optional): Determine whether to clear the corresponding collection in the database before inserting data. The default is False.
        - user_name(str, optional): MongoDB username. This parameter is optional. By default, the username and password mode are not used
        - password(str, optional): MongoDB password
        - cfg_file(str,optional): use config file property

    Methods:
        - assure_empty(): Empty the specified collection in the database. If db_check is set to True, this method is called before the data is automatically inserted.
        - insert(data: iter, buffer_size=10000, accelerate=False, counter=False): Inserts data from the specified csv file into the corresponding collection.
        - clear_collection(): Clear collection
    """

    def __init__(self, db_name: str, collection_name: str, host: str = "127.0.0.1", port: int = 27017,
                 username: str = None, password: str = None, db_check=True, cfg_file=None):
        if cfg_file is None:
            pass
        else:
            config = configparser.ConfigParser()
            config.read(cfg_file)
            host = config.get('dbserver', 'host')
            port = int(config.get('dbserver', 'port').strip())
            username = config.get('dbserver', 'user')
            password = config.get('dbserver', 'password')

        self.client = pymongo.MongoClient(host, port) \
            if username is None and password is None \
            else pymongo.MongoClient(host, port, username=username, password=password)
        self.db = self.client[db_name]
        self.collection_name = collection_name
        self.collection = self.db[collection_name]
        self.assure_empty(db_check)

    def insert(self, data: iter, buffer_size=10000, accelerate=False, counter=False) -> None:
        """
            insert a well-organized dict items, so generator is recommended.
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
                logging.debug("Collection is empty!")
            else:
                logging.error("Collection not empty.")
                raise Exception("Collection not empty Error!")

    def clear_collection(self):
        self.collection.drop()

    def search_record(self, query: dict) -> dict:
        return self.collection.find_one(query)

    def close(self):
        self.client.close()


if __name__ == "__main__":
    # The following is a usage example
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    # insert data into collection "offsides" in database "drugdb" whose server at "59.168.1.100"
    # disable collection empty check
    drugDB = DBConnection("drugdb", "offsides", "59.168.1.100", username="root", password="pw123", db_check=False)
    # If you use config
    drugDB = DBConnection("drugdb", "offsides", cfg_file="../../conf/drugkb.config")

    """
    Recommend Usage
    # large data case, you should pass a generator
    def generate_object():
        for row in db_reader:
            # wrapped as a dict object (IMPORTANT)
            drug = dict(zip(header, [row[i] for i in field_allowed]))
            yield drug

    db.insert(generate_object, accelerate=True)

    # normal size data, pass a list is ok
    db.insert([dict1,dict2,...], accelerate=True)
    """
