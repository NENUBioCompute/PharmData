"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/20/2024 10:56 PM
  @Email: deepwind32@163.com
"""
import multiprocessing

from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser

collection_list = ['source_bindingdb', 'source_biogrid', 'source_brenda_enzyme', 'source_dgidb_categories',
                   'source_dgidb_drugs', 'source_dgidb_genes', 'source_dgidb_interactions', 'source_do',
                   'source_drugbank', 'source_drugs', 'source_faers', 'source_genbank_gene',
                   'source_genbank_pubmed', 'source_icd11', 'source_kegg_compounds', 'source_kegg_drugs',
                   'source_kegg_ddi', 'source_kegg_diseases', 'source_kegg_drugs', 'source_kegg_pathway',
                   'source_twosides_offsides', 'source_twosides_twosides', 'source_pharmgkb_drugs',
                   'source_pharmgkb_genes', 'source_pharmgkb_pathways', 'source_pharmgkb_relationships',
                   'source_smpdb_metabolites', 'source_smpdb_pathways', 'source_smpdb_proteins',
                   'source_smpdb_sbml', 'source_stitch_actions', 'source_stitch_chemical_chemical_links',
                   'source_stitch_protein_chemical_links', 'source_transporter', 'source_ttd_biomarker2disease',
                   'source_ttd_crossmatching', 'source_ttd_drug', 'source_ttd_drug2disease', 'source_ttd_synonyms',
                   'source_ttd_target', 'source_ttd_target2disease', 'source_ttd_target2drug',
                   'source_ttd_target2kegg', 'source_ttd_target2wiki', 'source_uniprot', 'source_wikipathway']

cfg_file = "../conf/drugkb.config"
config = ConfigParser(cfg_file)


def dump(config, col):
    db = DBConnection("PharmRG", "source_drugbank", empty_check=False, config=config)
    db.print_struct_fields(col, save_as_file=True, file_path="./dump")
    db.close()


# each process will use 10% of cpu resource of mongo, 8 is about to reach the performance limitation
# warning: this operation will consume all memory if mongodb hasn't set the memory limitation
# After running this program, restart mongodb server would be highly recommended.
pool = multiprocessing.Pool(processes=8)
for col in collection_list:
    pool.apply_async(dump, (config, col))

pool.close()
pool.join()
print("all collections dump successfully.")
