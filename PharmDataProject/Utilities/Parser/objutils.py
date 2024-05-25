""" Methods to update objects for better data representation in databases.

 xmltodict library is a reliable library that most of the xml datasets supported
 by nosqlbiosets project are parsed with.
 For various reasons source xml files may have extra layers of names, especially
 for list attributes, e.g. obj->genes->gene. Here unifylistattribute method
 remove the last name 'gene' to simplify browsing and querying the data
 from the databases.
 """
from _collections import OrderedDict


# Make sure type of given attribute is list
# 之前xxxs下面存的是一个OrderedDict{xxx: [x1,x2,x3]}，现在是想把xxxs下面直接存成[x1,x2,x3]这种list；

def unifylistattribute(e, listname, objname, renamelistto=None):
    if e is None:
        return

    if listname in e:
        if e[listname] is None:
            del e[listname]  # 删除e（传入的OrderedDict）中的键值为空的key-value
        else:
            newlist = listname if renamelistto is None else renamelistto
            if e[listname].get(objname):
                if isinstance(e[listname][objname], list):
                    e[newlist] = e[listname][objname]
                else:
                    e[newlist] = [e[listname][objname]]
                if renamelistto is not None:
                    del e[listname]


# Make sure type of given attributes are list
# List attribute names are assumed to end with 's', and object names
# are equal to the list names without 's', such as genes vs gene
def unifylistattributes(e, list_attrs):
    for listname in list_attrs:
        # print(listname)
        objname = listname[:-1]  # 去掉list_attrs中每个元素后面的s
        # print(objname)
        if isinstance(e, list):
            for item_e in e:
                unifylistattribute(item_e, listname, objname)
        else:
            unifylistattribute(e, listname, objname)


# Make sure type of boolean attributes are boolean
def checkbooleanattributes(e, attrs):
    if e is None:
        return
    for attr in attrs:
        if attr in e:
            if not isinstance(e[attr], bool):
                if e[attr] in ['true', 'True']:
                    e[attr] = True
                else:
                    e[attr] = False


# Make sure type of numeric attributes are numeric
def num(e, attr, ntype=int):
    if attr in e:
        if isinstance(e[attr], str) and len(e[attr]) == 0:
            del e[attr]
        else:
            r = ntype(e[attr])
            e[attr] = r
            return r
