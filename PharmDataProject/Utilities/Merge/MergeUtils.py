"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 5/27/2024 2:16 PM
  @Email: deepwind32@163.com
"""
import time

import pubchempy as pcp
from tqdm import tqdm


class MergeUtils:
    @staticmethod
    def merge_with_pubchem(thread_id, id_queue, id_mapping, share_counter, locks, not_found_list, bar=None,
                           id_type="name"):
        """
        use with Thread, fetch pubchem cid and sid with giving info
        :param thread_id: an identifier
        :param id_queue: (id, info)
        :param id_mapping: {id: (cid, sid)}
        :param share_counter: a counter shared with threads
        :param locks: locks
        :param not_found_list: to output the not found id
        :param bar: tqdm bar
        :param id_type: "all" or "cid" or "sid" or "sid2cid"
        """
        share_counter.setdefault("inner_count", {})[thread_id] = 0

        while not id_queue.empty():
            with locks["all_lock"]:
                share_counter["all_count"] += 1
            share_counter["inner_count"][thread_id] += 1
            execute_flag = True
            max_retry = 3

            id, info = id_queue.get()
            counter_add_flag = {"cid": False, "sid": False}
            while execute_flag and max_retry:
                # avoid counter mistake caused by retry
                max_retry -= 1
                if counter_add_flag["cid"]:
                    with locks["cid_lock"]:
                        share_counter["cid_count"] -= 1
                    counter_add_flag["cid"] = False

                if counter_add_flag["sid"]:
                    with locks["sid_lock"]:
                        share_counter["sid_count"] -= 1
                    counter_add_flag["sid"] = False

                if counter_add_flag["cid"] or counter_add_flag["sid"]:
                    share_counter["all_count"] -= 1

                pubchem_cid, pubchem_sid = None, None
                try:
                    if id_type in ("name", "cid"):
                        # archive cid by info
                        try:
                            pubchem_cid = pcp.get_compounds(f"{info}", id_type, timeout=3)[0].cid
                            if pubchem_cid:
                                with locks["cid_lock"]:
                                    share_counter["cid_count"] += 1
                                    counter_add_flag["cid"] = True
                        except (pcp.BadRequestError, IndexError):
                            pass

                    if id_type == "name" or "sid" in id_type:
                        # archive sid by info
                        search_type = id_type if id_type != "sid2cid" else "sid"
                        try:
                            substance = [i for i in pcp.get_substances(f"{info}", search_type, timeout=3)]
                            pubchem_sid = [i.sid for i in substance]
                            if pubchem_sid:
                                with locks["sid_lock"]:
                                    share_counter["sid_count"] += 1
                                    counter_add_flag["sid"] = True
                            if id_type == "sid2cid":
                                substance = substance[0]
                                try:
                                    if substance.standardized_compound:
                                        pubchem_cid = substance.standardized_compound.cid
                                        if pubchem_cid:
                                            with locks["cid_lock"]:
                                                share_counter["cid_count"] += 1
                                                counter_add_flag["cid"] = True
                                except KeyError:
                                    pass
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
                    if str(e) == "'PUGREST.ServerBusy'":
                        max_retry += 1
                    else:
                        print(id, e)
                        id_queue.put((id, info))
                        execute_flag = False
                    time.sleep(1)

            if bar is not None:
                bar.update(1)

            if max_retry < 0:
                raise Exception(f"Achieve id {id} failed")

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
