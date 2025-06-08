import os
import re

import json
import xml.etree.ElementTree as ET
from lxml import etree
import xmltodict
import argparse
from neo4j import GraphDatabase, RoutingControl
from neo4j.exceptions import Neo4jError

parser = argparse.ArgumentParser(description="Arguments for the function")
# Add arguments

parser.add_argument("--input_directory", type=str, required=True, help="Xsd schema path")
parser.add_argument("--output_directory", type=str, required=True, help="Json file path")
parser.add_argument("--file_extension", type=str, required=True, help="File extension to process (e.g., .xsd)")
parser.add_argument("--cypher_script_path", type=str, help="Path to the Cypher script for generating queries")
parser.add_argument("--database", type=str,  help="Neo4j database name")
parser.add_argument("--neo4j_uri", type=str, default="bolt://localhost:7687", help="Neo4j URI")

args = parser.parse_args()

input_directory = args.input_directory
output_directory = args.output_directory
file_extension = args.file_extension
GENERATION_CYPHER_SCRIPT_PATH = args.cypher_script_path
DBName = args.database
#AUTH = (args.neo4j_username, args.neo4j_password)
AUTH = ("neo4j", "password")
URI = args.neo4j_uri

def run_query_script(driver):
    try:
        # Read the raw file content
        with open(GENERATION_CYPHER_SCRIPT_PATH, 'r', encoding='utf-8') as infile:
            content = infile.read()

    except Exception as ex:
        # Handle unexpected errors
        print(f"Unexpected error: {ex}")
        return {}

    # Split content into individual queries on lines starting with ';'
    queries = re.split(r'^\s*;\s*$', content, flags=re.MULTILINE)
    results = []
    for query in queries:
        query = query.strip()
        if not query:
            continue
        try:
            with driver.session(database=DBName) as session:
                params = {"importPath": output_directory}
                # Replace backslashes with forward slashes in importPath
                params["importPath"] = params["importPath"].replace("\\", "/")
                records = session.run(query, params)

                for record in records:
                    print(record)
                    results.append(record.data())

        except Neo4jError as e:
            # Handle Neo4j-specific errors
            print(f"Neo4j error: {e}")
            return {}

        except ValueError as ve:
            # Handle validation errors
            print(f"Validation error: {ve}")
            return {}

        except Exception as ex:
            # Handle unexpected errors
            print(f"Unexpected error: {ex}")
            return {}

    return results


# Function to remove all unnecessary whitespace from XML
def clean_and_dump_to_json_file(file_path, output_file_path):
    # Parse the XML file using lxml to preserve all attributes
    tree = etree.parse(file_path)
    root = tree.getroot()

    # Convert the XML tree to a string
    xml_string = etree.tostring(root, encoding="unicode", pretty_print=False)

    # Convert XML to a Python dictionary with attributes
    xml_dict = xmltodict.parse(xml_string, dict_constructor=dict)

    # Remove null properties from the dictionary
    xml_dict = remove_null_properties(xml_dict)

    # Save the processed dictionary to the output file in JSON format to view structure
    with open(output_file_path, "w") as output_file:
        json.dump(xml_dict, output_file, indent=4)

    print(f"Successfully removed spaces and processed XML to {output_file_path}.")

# Recursive function to remove null properties
def remove_null_properties(d):
    if isinstance(d, dict):
        return {k: remove_null_properties(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [remove_null_properties(item) for item in d if item is not None]
    else:
        return d

# Function to process files recursively and preserve directory structure
def process_directory(input_dir, output_dir,file_extension):
    for root, dirs, files in os.walk(input_dir):
        # Create corresponding directory structure in output directory
        relative_path = os.path.relpath(root, input_dir)
        output_root = os.path.join(output_dir, relative_path)
        if not os.path.exists(output_root):
            os.makedirs(output_root)

        # Process all .xml files in the current directory
        for file in files:
            if file.endswith(file_extension):
                input_file_path = os.path.join(root, file)
                # Change the file extension from .xml to .json for the output
                output_file_path = os.path.join(output_root, file.replace(file_extension, ".json"))

                # Remove spaces and save to new directory
                clean_and_dump_to_json_file(input_file_path, output_file_path)

# Function to check if the directory exists
def check_directory(directory_path):
    if not os.path.exists(directory_path):
        print(f"Error: The directory '{directory_path}' does not exist.")
        return False
    return True

# Get the directory where the script is located
script_directory = os.path.dirname(os.path.abspath(__file__))

# Define relative input and output directories (using raw string and forward slashes)
#input_directory = os.path.join(script_directory, r"..", "schemas")
#output_directory = os.path.join(script_directory, r"..", "extracts", "schemas_json")

# Resolve paths to absolute paths
input_directory = os.path.abspath(input_directory)
output_directory = os.path.abspath(output_directory)

# Check if input directory exists before proceeding
if check_directory(input_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)  # Create output directory if it doesn't exist

    # Start processing
    process_directory(input_directory, output_directory, file_extension)
    print("Processing complete!")

    driver = GraphDatabase.driver(URI, auth=AUTH)

    run_query_script(driver)


else:
    print("Please provide a valid input directory.")
