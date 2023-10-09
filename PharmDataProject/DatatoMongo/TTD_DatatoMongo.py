# -*- coding: utf-8 -*-
# developer   ：zhangkeming
# date   ：2023/9/25  16:31
class DatatoMongdb:
    def __init__(self,host,port,username,password,
                 dbname,collection):
        self.host=host
        self.port=port
        self.username=username
        self.password=password
        self.dbname=dbname
        self.collection=collection



