#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 09 16:12:04 2024
1. Create a dataframe of our data.
2. Create a dataframe of their data.
3. Iterate through our data and the first match on their end should get an event ID associated with it.
4. Get a list of leftover event IDs.
5. Get a list of their data that doesn't have an event ID mapped to it.

@author: deeptanshupaul
"""

# Import statements
import pandas as pd 
import json 
import argparse  as ap
import datetime as dt
from datetime import timedelta

def load_file(filename):
    """
    This function loads data based on whether its a CSV or Excel file.
    
    Parameters
    ----------
    filename : string
        The path to the input file.

    Returns
    -------
    DataFrame
        The dataframe containing the data that was in the file.

    """
    if filename[-3:] == 'csv':
        return pd.read_csv(filename)
    elif filename[-4:] == 'xlsx':
        return pd.read_excel(filename)

def prepare_data(primary, secondary):
    """
    This function prepares the data for matching. It does the following steps:
        1. In the primary dataframe, it combines the given name and family name columns to get a "name" field.
        2. It extracts the amount from the primary dataframe's 'Reward Name' Field
        3. Converts the telephone number in the primary dataframe to a 10 digit number. 
        4. Converts all datetime fields into a dt.date() object.
        5. Adds a CASHVIP field to the primary dataframe.

    Parameters
    ----------
    primary : dataframe
        This is the dataframe of our data.
    secondary : dataframe
        This is the dataframe of Ardex/BAL

    Returns
    -------
    {'primary': primary, 'secondary': secondary}

    """
    
    # Combine the given name and family name fields to a single name field.
    primary['name'] = primary['Given Name'] + ' ' + primary['Family Name']
    
    # Extract the amount from the 'Reward Name' Field
    primary['amount'] = primary['Reward Name'].str.extract(r'£(\d+)').astype(float)
    
    # Convert the telephone number in the primary dataframe to a 10 digit number
    primary['Telephone Number'] = primary['Telephone Number'].astype(str).str[-10:]
    
    # Convert all datetime fields into a dt.date() object for both dataframes
    primary['Reported At'] = pd.to_datetime(primary['Reported At']).dt.date
    secondary['Order Release Date (mm/dd/yyyy)'] = pd.to_datetime(secondary['Order Release Date (mm/dd/yyyy)']).dt.date
    
    # Adds a CASHVIP field to the primary dataframe based on 'Reward Name'
    primary['CASHVIP'] = primary['Reward Name'].apply(lambda x: 'YES' if 'CASHVIP' in x else 'NO')
    
    return {'primary': primary, 'secondary': secondary}

    
    
def match_data(primary, secondary):
    """
    This function attaches an 'eventID' field to the secondary dataframe. 
    - This is arrived at by iterating through the primary dataframe and attempting to match the row to data present in the secondary dataframe. 
    - Each eventID is attached once in the secondary dataframe. There are no repeats.
    - Once the matching is done, two dataframes are created, one with the details of each unmatched row in the primary called missing_events and a second with the rewards that couldn't be matched, as missing_rewards
    

    Parameters
    ----------
    primary : TYPE
        DESCRIPTION.
    secondary : TYPE
        DESCRIPTION.
    

    Returns
    -------
    {'primary': primary, 'secondary': secondary, 'missing events': missing_events, 'missing rewards': missing_rewards}

    """
    # Adding a placeholder for matched Event IDs in the secondary dataframe
    secondary['Event ID'] = None
    
    # Initialize empty lists for unmatched entries
    unmatched_primary_indices = primary.index.tolist()
    unmatched_secondary_indices = secondary.index.tolist()

    # Iterate through primary dataframe
    for i, primary_row in primary.iterrows():
        for j, secondary_row in secondary[secondary.index.isin(unmatched_secondary_indices)].iterrows():
            # Check for match based on phone number and name
            if str(secondary_row['Recipient Phone Number']).endswith(primary_row['last_10_digits']) and primary_row['name'].lower() == secondary_row['Recipient'].lower():
                
                # Further checks for date range, amount, and CASHVIP if applicable
                if (secondary_row['Order Release Date (mm/dd/yyyy)'] > primary_row['Reported At']) and \
                    (secondary_row['Order Release Date (mm/dd/yyyy)'] <= primary_row['Reported At'] + timedelta(days=7)) and \
                    (primary_row['amount'] == secondary_row['Amount']) and \
                    ((primary_row['CASHVIP'] == 'YES' and secondary_row['CASHVIP?'] == 'YES') or \
                    (primary_row['CASHVIP'] == '' and pd.isna(secondary_row['CASHVIP?']))):
                    
                    # Assigning the Event ID and removing indices from unmatched lists
                    secondary.at[j, 'Event ID'] = primary_row['Event ID']
                    if i in unmatched_primary_indices:
                        unmatched_primary_indices.remove(i)
                    if j in unmatched_secondary_indices:
                        unmatched_secondary_indices.remove(j)
                    break  # Break the loop after finding the first match

    # Creating dataframes for unmatched entries
    missing_events = primary.loc[unmatched_primary_indices].copy()
    missing_rewards = secondary.loc[unmatched_secondary_indices].copy()

    # Removing helper columns for unmatched entries
    missing_rewards.drop(columns=['Event ID'], inplace=True)

    return {
        'primary': primary,
        'secondary': secondary.assign(secondary=secondary['Event ID'].notnull()),
        'missing_events': missing_events,
        'missing_rewards': missing_rewards
    }

def main():
    # Setting up Argument Parser
    parser = ap.ArgumentParser(description="Concatenate Excel and CSV files in a directory.")
    parser.add_argument("our_data", type=str, help="Path to the excel file containing our data")
    parser.add_argument("their_data", type=str, help="path to the excel sheet containing their data")
    args = parser.parse_args()

    # Load the files
    primary = load_file(args.our_data)
    secondary = load_file(args.their_data)
    
    # Validate and prepare data
    prepared_data = prepare_data(primary, secondary)

    output = match_data(prepared_data["primary"], prepared_data["secondary"])
    output["primary"].to_excel("primary.xlsx", index = False)
    output["secondary"].to_excel("secondary.xlsx", index = False)
    output["missing_events"].to_excel("missing_events.xlsx", index = False)
    output["missing_rewards"].to_excel("misisng_rewards.xlsx", index = False)

if __name__ == "__main__":
    main()
