import os
import libsbml
from pymongo import MongoClient
import math

mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'SMPDB_SBML'

def parse_and_save_all_sbml_to_mongodb(directory_path):
    # Connect to MongoDB
    client = MongoClient(host=mongo_host, port=mongo_port)
    db = client[database_name]
    collection = db[collection_name]

    # Iterate through all files in the specified directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".sbml"):
            sbml_file_path = os.path.join(directory_path, filename)

            # Create an SBML reader
            reader = libsbml.SBMLReader()

            # Read the SBML file
            document = reader.readSBMLFromFile(sbml_file_path)

            # Check for errors in reading the file
            if document.getNumErrors() > 0:
                print(f"Errors encountered while reading SBML file: {sbml_file_path}")
                print(document.getErrorLog().toString())
                continue

            # Get the SBML model from the document
            model = document.getModel()

            # Access various information from the model
            if model is not None:
                # Prepare a dictionary to store the model information
                model_info = {
                    "model_id": model.getId() or "",  # Replace None with an empty string if it's None
                    "num_compartments": model.getNumCompartments(),
                    "num_species": model.getNumSpecies(),
                    "num_reactions": model.getNumReactions(),
                    "num_parameters": model.getNumParameters(),
                    "compartments": [],
                    "species": [],
                    "reactions": [],
                    "parameters": []
                }

                # Store information about compartments
                for i in range(model.getNumCompartments()):
                    compartment = model.getCompartment(i)
                    compartment_id = compartment.getId() or ""
                    compartment_size = compartment.getSize()
                    size_value = "" if math.isnan(compartment_size) else compartment_size
                    model_info["compartments"].append({
                        "id": compartment_id,
                        "size": size_value
                    })

                # Store information about species
                for i in range(model.getNumSpecies()):
                    species = model.getSpecies(i)
                    species_id = species.getId() or ""
                    compartment_id = species.getCompartment() or ""
                    model_info["species"].append({
                        "id": species_id,
                        "initial_amount": species.getInitialAmount(),
                        "compartment": compartment_id
                    })

                # Store information about reactions
                for i in range(model.getNumReactions()):
                    reaction = model.getReaction(i)
                    reaction_id = reaction.getId() or ""
                    reaction_info = {
                        "id": reaction_id,
                        "reactants": [
                            {"species": reactant.getSpecies() or "", "stoichiometry": reactant.getStoichiometry()} for
                            reactant in reaction.getListOfReactants()],
                        "products": [
                            {"species": product.getSpecies() or "", "stoichiometry": product.getStoichiometry()} for
                            product in reaction.getListOfProducts()]
                    }
                    model_info["reactions"].append(reaction_info)

                # Store information about parameters
                for i in range(model.getNumParameters()):
                    parameter = model.getParameter(i)
                    parameter_id = parameter.getId() or ""
                    parameter_value = parameter.getValue()
                    value = "" if math.isnan(parameter_value) else parameter_value
                    model_info["parameters"].append({
                        "id": parameter_id,
                        "value": value
                    })

                # Insert the model information into MongoDB
                collection.insert_one(model_info)

            else:
                print(f"No model found in SBML file: {sbml_file_path}")

    # Close the MongoDB connection
    client.close()


# Example usage
sbml_directory_path = r"C:\Users\win11\PycharmProjects\SMPDB\smpdb_sbml"
parse_and_save_all_sbml_to_mongodb(sbml_directory_path)
