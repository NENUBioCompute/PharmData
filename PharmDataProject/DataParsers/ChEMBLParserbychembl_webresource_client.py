"""
# @Time        : 2023/10/8
# @Author      : yvshilin
# @Email       : 3542804395@qq.com
# @FileName    : ChEMBLParserbychembl_webresource_client.py
# @Software    : PyCharm
# @ProjectName : ChEMBL
"""
import json
from chembl_webresource_client.new_client import new_client
import time

# every molecule
class FindAndAnalyzeData:
    def FindAllMoleculesData(self):
        molecule = new_client.molecule
        res = molecule.all().only(['molecule_chembl_id','pref_name','max_phase','molecule_properties','molecule_synonyms','molecule_type','molecule_structures'])
        time.sleep(2)
        return res

    def FindAllRelatedTargetsId(self, moleculeId):
        activity = new_client.activity
        activities = activity.filter(molecule_chembl_id=moleculeId).only('target_chembl_id')  # get related targets
        s = set()
        for act in activities:
            s.add(act['target_chembl_id'])
        return s

    def FindTargetInformation(self, targetId):
        target = new_client.target
        targetInformation = target.filter(target_chembl_id=targetId).only(['target_chembl_id', 'pref_name', 'organism'])
        return targetInformation

    def DataToJson(self, moleculeData):
        # convert data to JSON format
        data = []
        data.append(moleculeData)
        jsonData = [{'molecule_chembl_id': d['molecule_chembl_id'], 'pref_name': d['pref_name'], 'max_phase': d['max_phase'],'molecule_properties': d['molecule_properties'], 'molecule_synonyms': d['molecule_synonyms'],'molecule_type': d['molecule_type'], 'molecule_structures': d['molecule_structures']} for d in data]
        id = jsonData[0]['molecule_chembl_id']
        # exception handing
        properties = self.EmptyError(0, jsonData, 'molecule_properties')
        molformula = self.EmptyError(1, properties, 'full_molformula')
        mwt = self.EmptyError(1, properties, 'full_mwt')
        name = self.EmptyError(0, jsonData, 'pref_name')
        max = self.EmptyError(0, jsonData, 'max_phase')
        synonyms = self.EmptyError(0, jsonData, 'molecule_synonyms')
        type = self.EmptyError(0, jsonData, 'molecule_type')
        structures = self.EmptyError(0, jsonData, 'molecule_structures')
        smiles = self.EmptyError(1, structures, 'canonical_smiles')
        inchi = self.EmptyError(1, structures, 'standard_inchi')
        inchiKey = self.EmptyError(1, structures, 'standard_inchi_key')

        targetsId = self.FindAllRelatedTargetsId(id)
        num = 0
        dictAll = {}
        for tid in targetsId:
            num = num + 1
            targetInformation = self.FindTargetInformation(tid)
            targetInformation1 = targetInformation[0]
            dict = {}
            dict['target_chembl_id'] = targetInformation1['target_chembl_id']
            dict['pref_name'] = targetInformation1['pref_name']
            dict['organism'] = targetInformation1['organism']
            dictAll['target{n}'.format(n=num)] = dict
        completeJsonData = [
            {'molecule_chembl_id': id, 'pref_name': name, 'max_phase': max, 'molecular_formula': molformula,
             'molecular_weight': mwt, 'chembl_synonyms': synonyms, 'molecule_type': type,
             'canonical_smiles': smiles, 'standard_inchi': inchi, 'standard_inchi_key': inchiKey,
             'related_targets': dictAll}]
        return completeJsonData

    def EmptyError(self, num, list, key, default=''):
        if num == 0:
            try:
                value = list[0][key]
                return value
            except (TypeError, KeyError):
                return default
        else:
            try:
                value = list[key]
                return value
            except (TypeError, KeyError):
                return default

