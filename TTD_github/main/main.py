

from DataParsers import DataParsers
from DataToMongo import DataToMongo


if __name__ == '__main__':

    name = 'D:/P1-04.xlsx'

    return_data1 = DataParsers(name1=name)



    DataToMongo(signal = 'A', list1 = 'B',host = "localhost", port = 27017, name=name, collection= 'TTDtest',return_data = return_data1)



