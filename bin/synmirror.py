#!/usr/bin/env python3

# Import packages
import argparse
import os
import synapseclient

# Parse CLI arguments
parser = argparse.ArgumentParser()
parser.add_argument("--objects")
parser.add_argument("--s3_prefix")
parser.add_argument("--parent_id")
parser.add_argument("--config")
parser.add_argument("--subfolder_prefix", default="subfolder_", help="Prefix for numeric subfolders")
args = parser.parse_args()

# Log into Synapse with access token or config file
if args.config is not None:
    syn = synapseclient.Synapse(configPath=args.config)
else:
    assert os.environ.get("SYNAPSE_AUTH_TOKEN") is not None
    syn = synapseclient.Synapse()
syn.login(silent=True)

# Define function for creating folders
def create_folder(name, parent_id):
    entity = {
        "name": name,
        "concreteType": "org.sagebionetworks.repo.model.Folder",
        "parentId": parent_id,
    }
    entity = syn.store(entity)
    return entity.id

# Helper function to manage folder creation and subfolder counting
def manage_subfolder_creation(parent_id, subfolder_counts, subfolder_prefix):
    # Initialize or increment subfolder index
    if parent_id not in subfolder_counts:
        subfolder_counts[parent_id] = 1
    else:
        # Check if current subfolder is full and increment to a new subfolder
        current_subfolder = f"{subfolder_prefix}{subfolder_counts[parent_id]}"
        current_subfolder_id = mapping.get(f"{parent_id}/{current_subfolder}/")
        if current_subfolder_id and subfolder_counts[current_subfolder_id] >= 10000:
            subfolder_counts[parent_id] += 1
    
    # Get new subfolder name based on count
    new_subfolder_name = f"{subfolder_prefix}{subfolder_counts[parent_id]}"
    return new_subfolder_name, subfolder_counts

# Iterate over S3 "folders"
s3_prefix = args.s3_prefix.rstrip("/") + "/"
mapping = {s3_prefix: args.parent_id}
subfolder_counts = {}  # Tracks the number of subfolders created under each parent and their counts
with open(args.objects, "r") as infile:
    for line in infile:
        object_uri = line.rstrip()
        head, tail = os.path.split(object_uri)
        head += "/"  # Keep trailing slash for consistency
        relhead = head.replace(s3_prefix, "")
        folder_uri = s3_prefix
        parent_id = args.parent_id
        for folder in relhead.rstrip("/").split("/"):
            if folder == "":
                continue
            
            # Check and manage numeric subfolder creation
            subfolder_name, subfolder_counts = manage_subfolder_creation(parent_id, subfolder_counts, args.subfolder_prefix)
            subfolder_uri = f"{parent_id}/{subfolder_name}/"
            if subfolder_uri not in mapping:
                subfolder_id = create_folder(subfolder_name, parent_id)
                mapping[subfolder_uri] = subfolder_id
                subfolder_counts[subfolder_id] = 0  # Initialize count for new subfolder
            parent_id = mapping[subfolder_uri]
            folder_uri += f"{folder}/"

            if folder_uri not in mapping:
                folder_id = create_folder(folder, parent_id)
                mapping[folder_uri] = folder_id
                subfolder_counts[folder_id] = 0  # Initialize count for new folder within subfolder

            subfolder_counts[parent_id] += 1  # Increment count for the current subfolder

        print(f"{object_uri},{mapping[folder_uri]}")
