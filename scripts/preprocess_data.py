"""Preprocess various types of COVID data

Currently, just census microdata.
"""
import numpy as np
import pandas as pd


def preprocess_census_data():
    """preprocess census data to clean CSV


    """

    """
    Load census data into individual arrays, one per variable.
    As the data is loaded, perform simple data cleaning.
        health_data : 1 excellent .... 5 poor
        county_data: county fips code e.g. 00004
            first two digits: state e.g. 01
            last three digits: county e.g. 004
        male_data: 1 male, 0 female, nan missing
    """
    health_data = [] # (1 excellent ... 5 poor)
    county_data = [] # county fips code, e.g. 00004
    male_data = [] # 1 male, 0 female, nan = missing

    """ Extract and recode data """
    DATA_P = "../data/cps_00001.dat"
    counter = 0
    for data_row in open(DATA_P, "r").readlines():

        # Counter.
        counter += 1
        if counter % 250000 == 0:
            print("\t...{}".format(counter))

        try:

            # Extract Data
            county = data_row[51:56]
            print(county)
            smoked_100_cigarettes = data_row[110:112]
            male = data_row[105]
            health = data_row[109]

            assert int(county[:2]) <= 50

            """ Recode Data """

            # Recode health -> (1-5 integer)
            health = int(health)
            assert (health > 0) and (health <= 5)

            # Recode male -> (0-1 integer)
            if male == "1":
                male = 1
            elif male == "2":
                male = 0
            else:
                male = np.nan

            """ Add data for this row to data structures """
            health_data.append(health)
            county_data.append(str(county))
            male_data.append(male)

        except Exception as e:
            print(counter, e)
            continue 

    """ 
    Average data by county, 
    casting the data to a pandas dataframe
    """

    # Cast to datafarme.
    data_dictionary = {"county_fips" : county_data, 
            "male" : male_data, 
            "health": health_data}
    individuals_census_dataframe = pd.DataFrame(data=data_dictionary)

    # Average data by county.
    counties_census_dataframe = individuals_census_dataframe.groupby(
            "county_fips").mean() 
    print(counties_census_dataframe)

    # Save.
    of_p = "../data/census_data_by_county.csv"
    counties_census_dataframe.to_csv(of_p)

def preprocess_distancing_data():
    """todo: make me"""
    
    # Load safegraph data.
    safegraph_data_p = "../data/safegraph/2020-04-24-social-distancing.csv"
    safegraph_dataframe = pd.read_csv(safegraph_data_p)
    """
    Summarize the data (by adding) as a function of the 
    county FIPS code, which is the first 5 digits of the
    12-digit block code.
        digits 1-2 = state
        digits 3-5 = county
        digits 6-11 = tract
        digit 12 = block group
    """
    
    # Add a column for county fips code
    county_fips_codes = []
    for full_fips_code in safegraph_dataframe["origin_census_block_group"].values:
        county_fips_code = str(full_fips_code)[:5]
        county_fips_codes.append(county_fips_code)
    safegraph_dataframe["county_fips"] = county_fips_codes
    
    # Summarize (by adding) the device_count and
    # completely_home_device_count by county. 
    keep_colnames = ["county_fips", "device_count", "completely_home_device_count"]
    safegraph_data_by_county = safegraph_dataframe[keep_colnames].groupby(
            by="county_fips").sum()
    print(safegraph_data_by_county)

    # Calculate a social distancing proportion for each block group, defined as 
    # the proportion of devices who stayed completely at home during the period.
    safegraph_data_by_county["proportion_stayed_at_home"] = (
            safegraph_data_by_county["completely_home_device_count"] / 
            safegraph_data_by_county["device_count"])
    
    # Save data.
    of_p = "../data/safegraph_aggregated_4_24_by_county.csv"
    write_colnames = ["proportion_stayed_at_home"]
    safegraph_data_by_county[write_colnames].to_csv(of_p)
    print(safegraph_data_by_county.head())

def merge_census_and_distancing_data():

    # Load census and distancing data.
    SAFEGRAPH_DATA_P = "../data/safegraph_aggregated_4_24_by_county.csv"
    CENSUS_DATA_P = "../data/census_data_by_county.csv"
    safegraph_dataframe = pd.read_csv(SAFEGRAPH_DATA_P)
    census_dataframe = pd.read_csv(CENSUS_DATA_P)

    # Preprocess the primary keys - FIPS codes - so that they match exactly.
    census_dataframe["county_fips"] = [str(unpadded_fips_code).zfill(5) 
            for unpadded_fips_code 
            in census_dataframe["county_fips"]] # zero-pad FIPS codes.
    safegraph_dataframe["county_fips"] = [str(unpadded_fips_code).zfill(5)
            for unpadded_fips_code
            in safegraph_dataframe["county_fips"]] # zero-pad FIPS codes.
    
    print(safegraph_dataframe.head())
    print(census_dataframe.head())
    
    # Merge census and distancing data.
    fips_to_proportion_stayed_at_home = {}
    for county_fips, proportion_stayed_at_home in safegraph_dataframe[["county_fips", "proportion_stayed_at_home"]].values:
        fips_to_proportion_stayed_at_home[county_fips] = proportion_stayed_at_home
    
    
    proportion_stayed_at_home = []
    for county_fips in census_dataframe["county_fips"]:
        if county_fips in fips_to_proportion_stayed_at_home:
            proportion_stayed_at_home.append(
                    fips_to_proportion_stayed_at_home[county_fips])
        else:
            proportion_stayed_at_home.append(np.nan)
    census_dataframe["proportion_stayed_at_home"] = proportion_stayed_at_home
    merged_dataframe = census_dataframe.dropna()

    # Write the merged dataframe.
    of_p = "../data/census_and_safegraph_data_merged.csv"
    merged_dataframe.to_csv(of_p)
    print(merged_dataframe.head())
    print(len(merged_dataframe))

if __name__ == "__main__":
    preprocess_census_data()
    #preprocess_distancing_data()
    #merge_census_and_distancing_data()
