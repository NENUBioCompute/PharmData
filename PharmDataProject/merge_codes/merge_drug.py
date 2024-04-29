"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/25/2024 3:38 PM
  @Email: deepwind32@163.com
"""
import threading
import time
from queue import Queue
from threading import Thread

import pubchempy as pcp

from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class MergeDrug:
    def __init__(self, config):
        self.config = config
        self.db = DBConnection(config.get("db_name", "dbserver"),
                               config.get("collection_name", "merge_drugs"),
                               config=config, empty_check=False)
        self.share_var = {}

    def merge_bindingdb(self):
        for i in self.db.get_iter(self.config.get("col_name_1", "bindingdb")):
            pass

    def merge_drugbank(self):
        self.share_var = {
            "all_count": 0,
            "cid_count": 0,
            "sid_count": 0,
            "not_found_count": 0,
            "inner_count": {}
        }
        mapping_list = []
        not_found_list = []
        all_lock = threading.Lock()
        cid_lock = threading.Lock()
        sid_lock = threading.Lock()
        save_lock = threading.Lock()

        def merge_with_pubchem(thread_id):
            self.share_var["inner_count"][thread_id] = 0

            while not id_queue.empty():
                self.share_var["inner_count"][thread_id] += 1

                id, unii = id_queue.get()

                # retry when encountering network issue
                execute_flag = True
                while execute_flag:
                    try:
                        with all_lock:
                            self.share_var["all_count"] += 1

                        pubchem_cid = None
                        pubchem_sid = None

                        # ignore exist record
                        try:
                            flag = False
                            record = self.db.find_one({"drugbank._id": id})
                            if record:
                                if record["pubchem"]['cid']:
                                    flag = True
                                    with cid_lock:
                                        self.share_var["cid_count"] += 1
                                elif record["pubchem"]['sid']:
                                    flag = True
                                    with sid_lock:
                                        self.share_var["sid_count"] += 1
                                if flag:
                                    with save_lock:
                                        print(f'all_count: {self.share_var["all_count"]} id: {id}')
                                    execute_flag = False
                                    break
                        except Exception:
                            pass

                        # archive cid by drugbank id
                        try:
                            pubchem_cid = pcp.get_compounds(f"{id}", "name")[0].cid
                            with cid_lock:
                                self.share_var["cid_count"] += 1
                        except (pcp.BadRequestError, IndexError):
                            pass

                        # archive cid by unii
                        if not pubchem_cid and unii:
                            try:
                                pubchem_cid = pcp.get_compounds(f"{unii}", "name")[0].cid
                                with cid_lock:
                                    self.share_var["cid_count"] += 1
                            except (pcp.BadRequestError, IndexError):
                                pass

                        # archive sid by drugbank id
                        try:
                            pubchem_sid = pcp.get_substances(f"{id}", "name")[0].sid
                            with sid_lock:
                                self.share_var["sid_count"] += 1
                        except (pcp.BadRequestError, IndexError):
                            pass

                        # above code running successfully, stop next iter
                        execute_flag = False

                        # add into list
                        with save_lock:
                            if pubchem_cid or pubchem_sid:
                                mapping_list.append({"pubchem": {"cid": pubchem_cid,
                                                                 "sid": pubchem_sid},
                                                     "drugbank": {"_id": id}
                                                     })
                            else:
                                not_found_list.append(id)
                                self.share_var["not_found_count"] += 1
                            print(f'thread_id: {thread_id}\n'
                                  f'inner_count: {self.share_var["inner_count"][thread_id]} id: {id}\n'
                                  f'all_count: {self.share_var["all_count"]} cid_count: {self.share_var["cid_count"]}\n'
                                  f'sid_count: {self.share_var["sid_count"]} not_found_count: {self.share_var["not_found_count"]}\n')

                    except Exception as e:
                        # FIXME manual restart need. The loop didn't successfully get the failed data.
                        time.sleep(3)
                        print(id, e)

        id_queue = Queue()
        for i in self.db.get_iter(self.config.get("col_name_1", "drugbank")):
            id_queue.put((i.get("_id"), i.get("unii")))

        threads = [Thread(target=merge_with_pubchem, args=(i + 1,)) for i in range(int(self.config.get("thread_num")))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        print(self.share_var["inner_count"])

        self.db.insert(mapping_list)

        with open("not_found.txt", "w") as f:
            for i in not_found_list:
                f.write(f"{i}\n")

    def start(self):
        pass


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg, "merge_drugs")
    md = MergeDrug(config)
    md.merge_drugbank()
