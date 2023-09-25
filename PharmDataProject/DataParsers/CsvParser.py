import logging
import time
import pymongo
import csv
from collections import deque


def csv_line_count(csv_path):
    with open(csv_path, 'r') as file:
        return sum(1 for _ in file)


class CsvParser:
    """
    The CsvParser class is used to process standard SQL-type CSV files and import the contents into MongoDB.

    Parameters:
        - db_name (str): The name of MongoDB database
        - csv_path (dict): Dictionary that contains the mapping between the collection name and the csv file path, such as {" Collection name 1": "CSV file path 1", "Collection name 2": "CSV file path 2"}.
        - db_url (str, optional): Database url, the default value is "localhost".
        - port (int, optional): Database port, the default value is 27017
        - db_check (bool, optional): Determine whether to clear the corresponding collection in the database before inserting data. The default is False.
        - user_name(str, optional): MongoDB username. This parameter is optional. By default, the username and password mode are not used
        - password(str, optional): MongoDB password
        - auto_insert (bool, optional): Determine whether csv data is automatically inserted into the database during initialization. If enabled, no other parameters can be set. The default is False.

    Methods:
        - assure_empty(): Empty the specified collection in the database. If db_check is set to True, this method is called before the data is automatically inserted.
        - set_target_col(col_name: str): Sets the target set for the current operation.
        - set_csv_property(newline='\n', encoding='utf-8', delimiter=','): Set the read properties of the csv file.
        - set_fields(fields_needed=None, fields_removed=None): Set the fields that you want to keep or delete.
        - insert_data(col_name: str, buffer_size=10000): Inserts data from the specified csv file into the corresponding collection.
        - auto_insert(): Automatically converts all given csv files to MongoDB collections.
    """

    def __init__(self, db_name: str, csv_path: dict, db_url: str = "localhost", port: int = 27017,
                 user_name: str = None, password: str = None, db_check=False,
                 auto_insert=False):
        self.client = pymongo.MongoClient(db_url, port) \
            if user_name is None and password is None \
            else pymongo.MongoClient(db_url, port, username=user_name, password=password)

        self.db = self.client[db_name]
        # self.collections, Example：{col_name:(col_object, csv_path)}
        self.collections = {}  # The collection objects to be processed and the corresponding csv file path are saved
        self.target_col_name = ""
        self.target_col_object = None  # current collection object
        self.csv_property = {"newline": '\n', "encoding": 'utf-8', "delimiter": ','}
        self.db_check = db_check
        self.selected_fields = []
        self.removed_fields = []

        for col_name in csv_path.keys():
            self.collections[col_name] = (self.db[col_name], csv_path[col_name])

        if auto_insert:
            self.auto_insert()

    def assure_empty(self, col_name: str = None) -> None:
        # Clears the specified collection in the database.
        # If db_check is set to True, this method is called before the data is automatically inserted.
        if col_name:
            self.collections[col_name][0].drop()
        else:
            self.target_col_object.drop()

    def set_target_col(self, col_name: str) -> None:
        """
        Sets the target set for the current operation.
        :param  col_name: The name of the target collection.
        :return: None
        """
        self.target_col_name = col_name
        self.target_col_object = self.collections[col_name][0]

    def set_csv_property(self, newline='\n', encoding='utf-8', delimiter=',') -> None:
        """
        Set the read properties of the CSV file
        :param newline: Delimiter between lines in the CSV file, default "\\n"
        :param encoding: CSV file encoding, default is "utf-8"
        :param delimiter: Delimiter between fields in the CSV file, default is ","
        :return: None
        """
        self.csv_property = {"newline": newline, "encoding": encoding, "delimiter": delimiter}

    def set_fields(self, fields_needed: list[str] = None, fields_removed: list[str] = None) -> None:
        """
        Set the fields that you want to keep or delete.
        :param fields_needed: Field to be reserved. The format is [field_name1, field_name2].
        :param fields_removed: Field to be deleted. The format is [field_name1, field_name2].
        :return: None
        """
        if fields_needed and fields_removed:
            logging.warning("Parameter error! The fields that need to be kept and the fields that need to be deleted cannot be given at the same time!")
            raise SyntaxError("The fields that need to be kept and the fields that need to be deleted cannot be given at the same time!")
        if not (fields_needed or fields_removed):
            return
        if fields_needed:
            self.selected_fields = fields_needed
        elif fields_removed:
            self.removed_fields = fields_removed

    def insert_data(self, col_name: str, buffer_size=10000) -> None:
        """
            Inserts data from the specified CSV file into the corresponding collection.
            :param col_name: The name of the collection you want to insert data into.
            :param buffer_size: The size of the buffer when data is inserted. The default is 10000
            :return: None
        """
        def bulk_insert_data():
            requests = [pymongo.InsertOne(doc) for doc in buffer]
            self.target_col_object.bulk_write(requests, ordered=False)
            buffer.clear()

        self.set_target_col(col_name)

        if self.target_col_object is None:
            logging.warning("No collection objects are set to be operated on")
            raise ValueError("No collection objects are set to be operated on")

        if self.db_check:
            self.assure_empty()

        logging.info(f"Collection {self.target_col_name} start conversion.")
        beginning_time = time.time()
        line_counter = csv_line_count(self.collections[col_name][1])

        with open(self.collections[col_name][1], 'r', newline=self.csv_property["newline"],
                  encoding=self.csv_property["encoding"]) as csvfile:
            db_reader = csv.reader(csvfile, delimiter=self.csv_property["delimiter"])
            header = next(db_reader)
            field_allowed = list(range(len(header)))
            try:
                if self.selected_fields:
                    field_allowed = [header.index(field) for field in self.selected_fields]
                elif self.removed_fields:
                    field_allowed = [i for i in field_allowed if
                                     i not in [header.index(field) for field in self.removed_fields]]
            except ValueError as e:
                logging.warning(e)
                raise ValueError("The specified field does not exist") from e
            header = [header[i] for i in field_allowed]
            logging.info(f"The reserved fields are {header}")
            buffer = deque()
            for row in db_reader:
                drug = dict(zip(header, [row[i] for i in field_allowed]))
                buffer.append(drug)
                if len(buffer) >= buffer_size:
                    bulk_insert_data()
            # Ensure that all data in the buffer is retrieved
            if buffer:
                bulk_insert_data()

        logging.info(
            f"Collection {self.target_col_name} conversion complete，Insert {line_counter} bar documents, time elapsed {time.time() - beginning_time:.2f}s")

    def auto_insert(self):
        """
        Automatically converts all given csv files to MongoDB collections.
        :return: None
        """
        for col_name in self.collections.keys():
            self.insert_data(col_name)
        logging.info("All csv files are processed")


if __name__ == "__main__":
    # The following is a usage example
    logging.basicConfig(filename="../log/db_log.txt", level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    path = {
        "offsides": r"D:\my files\Work\drug database\data\OFFSIDES.csv",
    }
    # drugDB = CsvParser("PharmRG", path, "59.73.198.168", user_name="readwrite", password="readwrite",
    #                    db_check=True, auto_insert=True)
    drugDB = CsvParser("PharmRG", path, db_check=True, auto_insert=True)
