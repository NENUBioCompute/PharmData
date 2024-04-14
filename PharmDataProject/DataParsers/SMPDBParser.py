"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:28
  @Email: 2665109868@qq.com
  @function
"""
import os
import pprint

import pandas as pd
import libsbml

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser

class SMPDBParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("smpdb", "data_path")

    def __csvs2dicts(self, dir_path: str):
        return [pd.read_csv(os.path.join(dir_path, filename)).to_dict(orient='records') for filename in os.listdir(dir_path)]

    def __sbmls2dicts(self, dir_path: str):
        print("test")
        for filename in os.listdir(dir_path):
            if filename.endswith(".sbml"):
                sbml_file_path = os.path.join(dir_path, filename)
                reader = libsbml.SBMLReader()
                document = reader.readSBMLFromFile(sbml_file_path)
                if document.getNumErrors() > 0:
                    continue
                model = document.getModel()
                if model:
                    model_info = dict()
                    # 收集模型基本信息
                    model_info['id'] = model.getId()

                    # 收集物种（ compartments, species）信息
                    compartments = {}
                    for c in model.getListOfCompartments():
                        compartments[c.getId()] = {'name': c.getName()}
                    model_info['compartments'] = compartments

                    species = {}
                    for s in model.getListOfSpecies():
                        species[s.getId()] = {
                            'name': s.getName(),
                            'compartment': s.getCompartment(),
                            'initial_concentration': s.getInitialConcentration(),
                            'boundary_condition': s.getBoundaryCondition()
                        }
                    model_info['species'] = species

                    reactions = {}
                    for r in model.getListOfReactions():
                        reaction_info = {}
                        reaction_info['id'] = r.getId()
                        reaction_info['name'] = r.getName()
                        reaction_info['reversible'] = r.getReversible()

                        # 收集反应物与产物及其系数
                        reactants = {s.getSpecies(): s.getStoichiometry() for s in r.getListOfReactants()}
                        products = {s.getSpecies(): s.getStoichiometry() for s in r.getListOfProducts()}

                        # 反应速率常数或公式
                        kinetic_law = r.getKineticLaw()
                        if kinetic_law is not None:
                            reaction_info['kinetic_law'] = kinetic_law.getFormula()

                        reaction_info['reactants'] = reactants
                        reaction_info['products'] = products

                        reactions[r.getId()] = reaction_info

                    model_info['reactions'] = reactions
                    pprint.pprint(model_info)
        return model_info

    def start(self):
        csv_dirs = ["smpdb_pathways", "smpdb_metabolites", "smpdb_proteins"]
        sbml_dir = "smpdb_sbml"
        # dicts = [self.__csv2dict(os.path.join(config.get("smpdb", "data_path"), dir_name)) for dir_name in csv_dirs]
        # dicts.append()
        # return
        model_info = self.__sbmls2dicts(os.path.join(config.get("smpdb", "data_path"), sbml_dir))
        pprint.pprint(model_info)





if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.GetConfig(cfg)
    SMPDBParser(config).start()