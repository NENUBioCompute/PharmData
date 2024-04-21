"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:28
  @Email: 2665109868@qq.com
  @function
"""
import os
import pprint

import libsbml
import pandas as pd
from tqdm import tqdm

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class SMPDBParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("smpdb", "data_path")

    def __csvs2dicts(self, dir_path: str):
        return [pd.read_csv(os.path.join(dir_path, filename)).to_dict(orient='records') for filename in
                tqdm(os.listdir(dir_path), desc="csv data processing")]

    def __sbmls2dicts(self, dir_path: str):
        all_model_info = []
        for filename in tqdm(os.listdir(dir_path), desc="csv data processing"):
            if filename.endswith(".sbml"):
                sbml_file_path = os.path.join(dir_path, filename)
                reader = libsbml.SBMLReader()
                document = reader.readSBMLFromFile(sbml_file_path)
                if document.getNumErrors() > 0:
                    continue
                model = document.getModel()
                if model:
                    model_info = dict()
                    model_info['id'] = model.getId()
                    # compartments
                    compartments = {}
                    for c in model.getListOfCompartments():
                        compartments[c.getId()] = {'name': c.getName()}
                    model_info['compartments'] = compartments
                    # species
                    species = {}
                    for s in model.getListOfSpecies():
                        species[s.getId()] = {
                            'name': s.getName(),
                            'compartment': s.getCompartment(),
                            'initial_concentration': s.getInitialConcentration(),
                            'boundary_condition': s.getBoundaryCondition()
                        }
                    model_info['species'] = species
                    # reactions
                    reactions = {}
                    for r in model.getListOfReactions():
                        reaction_info = {}
                        reaction_info['id'] = r.getId()
                        reaction_info['name'] = r.getName()
                        reaction_info['reversible'] = r.getReversible()

                        reactants = {s.getSpecies(): s.getStoichiometry() for s in r.getListOfReactants()}
                        products = {s.getSpecies(): s.getStoichiometry() for s in r.getListOfProducts()}

                        kinetic_law = r.getKineticLaw()
                        if kinetic_law is not None:
                            reaction_info['kinetic_law'] = kinetic_law.getFormula()

                        reaction_info['reactants'] = reactants
                        reaction_info['products'] = products

                        reactions[r.getId()] = reaction_info
                    model_info['reactions'] = reactions
                    all_model_info.append(model_info)
        return all_model_info

    def start(self):
        dirs = ["smpdb_pathways", "smpdb_metabolites", "smpdb_proteins", "smpdb_sbml"]
        for i, dir_name in enumerate(dirs):
            if i == 3:
                yield [dir_name, self.__sbmls2dicts(os.path.join(self.config.get("smpdb", "data_path"), dir_name))]
            else:
                yield [dir_name, self.__csvs2dicts(os.path.join(self.config.get("smpdb", "data_path"), dir_name))]


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.get_config(cfg)
    for i in SMPDBParser(config).start():
        pprint.pprint(i)
