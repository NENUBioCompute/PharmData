import configparser
import pymongo
from pymongo import MongoClient

class DBconnection(object):
    
    def __init__(self, dbname, host=None, port=None,
                 user=None, password=None):
        
        cfgfile = "../../conf/drugkb.config"
        config = configparser.ConfigParser()
        config.read(cfgfile)

        assert dbname is not None
        self.dbname = dbname
        
        if host is None:
            host = config.get('dbserver', 'host')
        if port is not None:
            port = config.get('dbserver', 'port')
        if user is None:
            user = config.get('dbserver', 'user')
        if password is None:
            password = config.get('dbserver', 'password')
        client = pymongo.MongoClient(host=host, port=port, username=user, password=password)
        self.my_db = client[self.db_name] # mongodb index
        # self.collection = self.my_db[myset]
        


        
        
        