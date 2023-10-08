def unifylistattribute(entry, attrname, subattrname):
    if attrname in entry:
        if not isinstance(entry[attrname], list):
            entry[attrname] = [entry[attrname]]
        for item in entry[attrname]:
            if subattrname in item:
                if not isinstance(item[subattrname], list):
                    item[subattrname] = [item[subattrname]]
                if len(item[subattrname]) == 1:
                    item[subattrname] = item[subattrname][0]


def unifylistattributes(entry, attrnames):
    for attrname in attrnames:
        unifylistattribute(entry, attrname, attrname[:-1])


def checkbooleanattribute(entry, attrname):
    if attrname in entry:
        if entry[attrname] in ['true', 'false']:
            entry[attrname] = entry[attrname] == 'true'


def checkbooleanattributes(entry, attrnames):
    for attrname in attrnames:
        checkbooleanattribute(entry, attrname)