
from pymongo import MongoClient


class DBconnection:
    def __init__(self, host, port, database, collection):
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.connection = None

    def connect(self):
        self.connection = MongoClient(self.host, self.port)
        db = self.connection[self.database]
        self.collection = db[self.collection]

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def getdrugid(self, entry):

        drug_name = entry.get('name', '')
        drug_category = entry.get('category', '')

        drug_id_string = drug_name + drug_category

        drug_id = hash(drug_id_string)

        return drug_id

    def reportprogress(self, is_completed):
        if is_completed:
            print("Progress: Completed")
        else:
            print("Progress: In progress")


if __name__ == "__main__":
    # 主函数
    host = "localhost"
    port = 27017
    database = "DrugBank"
    collection = "Drugdata2"

    db_connection = DBconnection(host, port, database, collection)
    db_connection.connect()

    xml_file = "full database.xml"
    entries = parse_drugbank_xmlfile(xml_file)

    for entry in entries:
        mongodb_index_entry(db_connection, entry)

    mongodb_indices(db_connection)

    db_connection.disconnect()