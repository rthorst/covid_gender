"""Preprocess various types of COVID data

Currently, just census microdata.
"""
import csv
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

def preprocess_census_data():
    """preprocess census data to clean CSV"""

    """Load census data. 

    The data are highly disaggregated; specifically, 
    there are many rows for each county, each row 
    corresponding to a specific subgroup.

    Thus, the next step will be to aggreagate (sum) 
    the rows across counties.

    We populate the following arrays, one per row of data:
        state_fips_codes []  # e.g. 01
        county_fips_codes [] # e.g. 107
        numbers_of_males = [] # e.g. 402135, raw count.
        numbers_of_females = [] e.g. 314314, raw count.
    """
    state_fips_codes = []
    county_fips_codes = []
    numbers_of_males = []
    numbers_of_females = []

    DATA_P = "../data/census-2018-pop.csv"
    f = open(DATA_P, "r", errors="ignore")
    r = csv.reader(f)

    STATE_COLNUM = 1
    COUNTY_COLNUM = 2
    MALE_COLNUM = 8
    FEMALE_COLNUM = 9

    header = next(r) # skip header
    for row in r:

        try:

            # extract data.
            state_fips_code = row[STATE_COLNUM]
            county_fips_code = row[COUNTY_COLNUM]
            number_of_males = int(row[MALE_COLNUM])
            number_of_females = int(row[FEMALE_COLNUM])

            # add data for this row to the data structures
            # containing data for all rows.
            state_fips_codes.append(state_fips_code)
            county_fips_codes.append(county_fips_code)
            numbers_of_males.append(number_of_males)
            numbers_of_females.append(number_of_females)

        except Exception as e:
            print(e)


    """Aggregate the data by county.

    Since many rows correspond to each county, the rows
    need to be aggregated by county, which is defined by a 
    5-digit code, state_fips + county_fips, e.g. 01 + 506 -> 01506

    This involves casting the data to a dataframe and adding the 
    data by county. Note that in the resulting dataframe, the index
    (df.index) will house the 5-digit county fips code.
    """

    # Write each individual row of data to a high-level dataframe
    # object, for easier summarization. 
    state_plus_county_fips = [
            state_fips + county_fips 
            for state_fips, county_fips 
            in zip(state_fips_codes, county_fips_codes)
            ]
    individual_rows_data_dictionary = {
        "county_fips" : state_plus_county_fips # 5 digits: state+county.
        "num_males" : numbers_of_males,
        "num_females" : numbers_of_females
        }
    individual_rows_dataframe = pd.DataFrame(data=individual_rows_data_dictionary)

    # Summarize data by county, by adding multiple rows corresponding
    # to the same county.
    census_data_by_county = individual_rows_dataframe.groupby("county_fips").sum()

    # Calculate proportion male column (male / male+female)
    proportions_male = [num_males / (num_males + num_females)
            for num_males, num_females in 
            zip(census_data_by_county.num_males, 
            census_data_by_county.num_females)]
    census_data_by_county["proportion_male"] = proportions_male


    """Write gender by county to disk.

    Note that we must be very careful with county fips codes, which 
    can be 0-padded and must have width 5. 

    Thus we write the dataframe 'by hand', ensuring that county fips 
    codes are formatted properly.
    """

    # Write output.
    of_p = "../data/census_gender_by_county.csv"
    with open(of_p, "w", newline="") as of:
    
        csv_writer_object = csv.writer(of)
        header = ["county_fips", "prop_male"]
        csv_writer_object.writerow(header)
        
        for county_fips, prop_male in zip(census_data_by_county.index, census_data_by_county.proportion_male):
            county_fips_padded = str(county_fips).zfill(5)
            data_row = [county_fips_padded, prop_male]
            csv_writer_object.writerow(data_row)

def preprocess_distancing_data():
    
    # Load safegraph data.
    safegraph_data_p = "../data/safegraph/2020-04-23-social-distancing.csv"
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
    CENSUS_DATA_P = "../data/census_gender_by_county.csv"
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


def analyze_merged_data():

    # load merged data.
    df = pd.read_csv("../data/census_and_safegraph_data_merged.csv")

    # correlate gender with staying at home
    rho, p = spearmanr(df.prop_male, df.proportion_stayed_at_home)
    print("correlation of male with stay at home, rho = {:.4f} p = {:.4f}".format(rho, p))
    print(len(df))

if __name__ == "__main__":
    #preprocess_census_data()
    preprocess_distancing_data()
    merge_census_and_distancing_data()
    analyze_merged_data()
