"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/25/2024 3:38 PM
  @Email: deepwind32@163.com
"""
import json
import threading
import time
from queue import Queue
from threading import Thread

import pubchempy as pcp
from tqdm import tqdm

from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class MergeUtils:
    @staticmethod
    def merge_with_pubchem(thread_id, id_queue, id_mapping, share_counter, locks, not_found_list, bar=None):
        """
        use with Thread, fetch pubchem cid and sid with giving info
        :param thread_id: an identifier
        :param id_queue: (id, info)
        :param id_mapping: {id: (cid, sid)}
        :param share_counter: a counter shared with threads
        :param locks: locks
        :param not_found_list: to output the not found id
        """
        share_counter.setdefault("inner_count", {})[thread_id] = 0

        while not id_queue.empty():
            share_counter["inner_count"][thread_id] += 1
            execute_flag = True
            max_retry = 3
            id, info = id_queue.get()

            while execute_flag and max_retry:
                max_retry -= 1
                pubchem_cid, pubchem_sid = None, None
                try:
                    # archive cid by info
                    try:
                        pubchem_cid = pcp.get_compounds(f"{info}", "name")[0].cid
                        with locks["cid_lock"]:
                            share_counter["cid_count"] += 1
                    except (pcp.BadRequestError, IndexError):
                        pass

                    # archive sid by info
                    try:
                        pubchem_sid = [i.sid for i in pcp.get_substances(f"{info}", "name")]
                        with locks["sid_lock"]:
                            share_counter["sid_count"] += 1
                    except pcp.BadRequestError:
                        pass

                    # above code running successfully, stop next iter
                    execute_flag = False

                    # add into list
                    with locks["save_lock"]:
                        if pubchem_cid or pubchem_sid:
                            id_mapping[id] = {"cid": pubchem_cid, "sid": pubchem_sid}
                        else:
                            not_found_list.append(id)
                            share_counter["not_found_count"] += 1

                except Exception as e:
                    print(id, e)
                    time.sleep(3)
            if bar is not None:
                bar.update(1)
            if max_retry < 0:
                raise Exception(f"Achieve id {id} failed")
        return id_mapping

    @staticmethod
    def merge_id_record(db, pubchem_id_mapping, merge_dbname):
        insert_list = []
        for id, value in tqdm(pubchem_id_mapping.items(), desc="updating database"):
            target = None
            # merge according cid
            if value.get("cid", None):
                target = db.find_one({"pubchem.cid": value["cid"]})
            else:
                # merge according sid
                for sid in value.get("sid", []):
                    target = db.find_one({"pubchem.sid": sid})
                    if target:
                        break

            if target:
                all_sid = target["pubchem"]["sid"]
                all_sid.extend(value.get("sid", []))
                query = {"_id": target["_id"]}
                update_operation = {
                    '$set': {
                        "pubchem": {"cid": target["pubchem"]["cid"], "sid": list(set(all_sid))},
                        merge_dbname: {"_id": id}
                    }
                }
                db.update_one(query, update_operation)
            else:
                insert_list.append({
                    "pubchem": {
                        "cid": value.get("cid", None),
                        "sid": value.get("sid", None)
                    },
                    merge_dbname: {"_id": id}
                })
        db.insert(insert_list)


class MergeDrug:
    def __init__(self, config):
        self.config = config
        self.db = DBConnection(config.get("db_name", "dbserver"),
                               config.get("collection_name", "merge_drugs"),
                               config=config, empty_check=False)
        self.share_var = {}

    def merge_ttd(self):
        # init var
        share_var = {
            "all_count": 0,
            "inchi_count": 0,
            "smiles_count": 0,
            "name_count": 0,
            "not_found_count": 0,
        }
        inner_var = {
            "all_count": 0,
            "cid_count": 0,
            "sid_count": 0,
            "not_found_count": 0,
        }

        id_queue = Queue()
        locks = {"all_lock": threading.Lock(),
                 "cid_lock": threading.Lock(),
                 "sid_lock": threading.Lock(),
                 "save_lock": threading.Lock()}

        not_found = []

        # get id mapping from ttd_crossmatching
        pubchem_id_mapping, name_list = self.merge_drug_cm()
        for item in name_list:
            id_queue.put(item)

        # collect info and put into id_queue
        for i in tqdm(self.db.get_iter(self.config.get("col_name_2", "ttd")), desc="reading ttd_drug"):
            share_var["all_count"] += 1

            id = i.get("DRUG__ID").get("value")
            if id in pubchem_id_mapping.keys():
                continue

            inchi_key = i.get("DRUGINKE")
            if inchi_key and inchi_key.get("value"):
                share_var["inchi_count"] += 1
                id_queue.put((id, inchi_key.get("value")))
                continue

            smiles = i.get("DRUGSMIL")
            if smiles and smiles.get("value"):
                share_var["smiles_count"] += 1
                id_queue.put((id, smiles.get("value")))
                continue

            name = i.get("TRADNAME")
            if name and name.get("value"):
                share_var["name_count"] += 1
                id_queue.put((id, name.get("value")))
                continue

            share_var["not_found_count"] += 1

        # search pubchem id with giving info
        id_mapping = {}
        bar = tqdm(desc="searching pubchem")
        threads = [Thread(target=MergeUtils.merge_with_pubchem, args=
        (i, id_queue, id_mapping, inner_var, locks, not_found, bar))
                   for i in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        bar.close()

        # if some id don't find corresponding pubchem id
        if not_found:
            print(f"{len(not_found)} ids can not be found.")
            with open("ttd_cm_not_found_id.txt", "w") as f:
                json.dump(not_found, f, indent=4)

        pubchem_id_mapping.update(id_mapping)
        print("ttd_drug_count", share_var, sep="\n")
        print("ttd_drug_cm_count", inner_var, sep="\n")
        print(f"found: {share_var['all_count'] - share_var['not_found_count'] - inner_var['not_found_count']}")
        MergeUtils.merge_id_record(self.db, pubchem_id_mapping, "ttd")

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
                max_retry = 3
                id, unii = id_queue.get()

                # retry when encountering network issue
                execute_flag = True
                while execute_flag and max_retry:
                    max_retry -= 1
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
                        except (pcp.BadRequestError, IndexError):
                            pass

                        # archive cid by unii
                        if not pubchem_cid and unii:
                            try:
                                pubchem_cid = pcp.get_compounds(f"{unii}", "name")[0].cid
                            except (pcp.BadRequestError, IndexError):
                                pass

                        if pubchem_cid:
                            with cid_lock:
                                self.share_var["cid_count"] += 1

                        # archive sid by drugbank id
                        try:
                            pubchem_sid = [i.sid for i in pcp.get_substances(f"{id}", "name")]
                            if pubchem_sid:
                                with sid_lock:
                                    self.share_var["sid_count"] += 1
                        except pcp.BadRequestError:
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

                if not execute_flag and not max_retry:
                    print(f"id {id}, exceed max retry")
                    with save_lock:
                        not_found_list.append(id)
                        self.share_var["not_found_count"] += 1

        id_queue = Queue()
        for i in tqdm(self.db.get_iter(self.config.get("col_name_1", "drugbank")), desc="read drugbank"):
            id_queue.put((i.get("_id"), i.get("unii")))

        threads = [Thread(target=merge_with_pubchem, args=(i + 1,)) for i in range(int(self.config.get("thread_num")))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        print(self.share_var["inner_count"])

        self.db.insert(mapping_list)

        with open("drugbank_not_found.txt", "w") as f:
            for i in not_found_list:
                f.write(f"{i}\n")

        print("drugbank finish merging")

    def start(self):
        pass

    def merge_drug_cm(self):
        pubchem_id_mapping = {}
        name_list = []

        for i in tqdm(self.db.get_iter(self.config.get("col_name_3", "ttd")), desc="reading ttd_crossmatching"):
            result = {}
            id = i.get("TTDDRUID").get("value")
            id_flag = True
            cid = i.get("PUBCHCID")
            if cid and cid.get("value"):
                result["cid"] = cid
                id_flag = False
            sid = i.get("PUBCHSID")
            if sid and sid.get("value"):
                result["sid"] = sid
                id_flag = False

            if id_flag:
                name = i.get("DRUGNAME")
                if name and name.get("value"):
                    name_list.append((id, name.get("value")))
                    continue
            else:
                pubchem_id_mapping[id] = result

        return pubchem_id_mapping, name_list


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg, "merge_drugs")
    md = MergeDrug(config)
    # assume collection merge_drugs is empty or a exception will be raised
    # md.merge_drugbank()
    md.merge_ttd()
