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

    # Calculate the 5-digit county fips code for each row of data,
    # by concatenating the 2-digit state code with the 3-digit county
    # code.
    state_plus_county_fips = [
            state_fips + county_fips 
            for state_fips, county_fips 
            in zip(state_fips_codes, county_fips_codes)
            ]
    
    # Aggregate to count the number of males and females in each 
    # 5-digit county fips code.
    county_fips_to_number_of_males = {fips : 0 
            for fips in state_plus_county_fips}
    county_fips_to_number_of_females = {fips : 0 
            for fips in state_plus_county_fips}
    disaggregated_data_iterator = zip(state_plus_county_fips, 
            numbers_of_males, 
            numbers_of_females)
    
    for county_fips, number_of_males, number_of_females in disaggregated_data_iterator:
        county_fips_to_number_of_males[county_fips] += number_of_males
        county_fips_to_number_of_females[county_fips] += number_of_females
    
    # Transform raw gender counts to proportions for each county.
    county_fips_to_proportion_male = {}
    for county_fips in state_plus_county_fips:
        num_males = county_fips_to_number_of_males[county_fips]
        num_females = county_fips_to_number_of_females[county_fips]
        proportion_male = (num_males / (num_males + num_females))

        county_fips_to_proportion_male[county_fips] = proportion_male

    """Write gender by county to disk.
    """
    
    of_p = "../data/census_gender_by_county.csv"
    with open(of_p, "w", newline="") as of:
    
        # Write header.
        csv_writer_object = csv.writer(of)
        header = ["county_fips", "prop_male"]
        csv_writer_object.writerow(header)
        
        # Write individual rows of data.
        for county_fips, proportion_male in county_fips_to_proportion_male.items():
            data_row = [county_fips, proportion_male]
            csv_writer_object.writerow(data_row)



def preprocess_distancing_data(date="04-23"):
    """preprocess social distacing data

    parameters
    -----------
    date (string) e.g. 04-23 which should be reflected in the file prefix.

    output file
    -----------
    ../data/safegraph_aggregated_{}_by_county.csv
    """
    
    # Load safegraph data.
    safegraph_data_p = "../data/safegraph/2020-{}-social-distancing.csv".format(date)
    safegraph_dataframe = pd.read_csv(safegraph_data_p)
    
    """Aggregate the data by county
    
    The data are disaggregated to census block codes (12-digit codes).
    The first five digits of the code indicate the county 
        (1-2 : state   ....   3-5 : county)

    Thus we extract the county for each census block and add different
    rows belonging to the same county.
    """
    
    # Extract county fips codes.
    county_fips_codes = []
    for full_fips_code in safegraph_dataframe["origin_census_block_group"].values:
        county_fips_code = str(full_fips_code)[:5]
        county_fips_codes.append(county_fips_code)
    
    # Summarize phone counts by county.
    # device_count is the total number of phones for people who live in this area.
    # completely_home_device_count is number of those phones that never left home
    #   during the specific time period.
    county_fips_to_device_count = {fips: 0 for fips in county_fips_codes}
    county_fips_to_completely_home_count = {fips: 0 for fips in county_fips_codes}
    county_data_iterator = zip(
            county_fips_codes,
            safegraph_dataframe.device_count.values,
            safegraph_dataframe.completely_home_device_count.values
            )
    for county_fips, device_count, completely_home_count in county_data_iterator:
        county_fips_to_device_count[county_fips] += device_count
        county_fips_to_completely_home_count[county_fips] += completely_home_count

    # Calculate the proportion of people in each county who stayed completel 
    # at home.
    county_fips_to_proportion_completely_at_home = {}
    for county_fips, device_count in county_fips_to_device_count.items():
        
        completely_home_count = county_fips_to_completely_home_count[county_fips]
        prop_completely_at_home = (completely_home_count / device_count)

        county_fips_to_proportion_completely_at_home[county_fips] = (
                prop_completely_at_home)

    # Save data.
    of_p = "../data/safegraph_aggregated_{}_by_county.csv".format(date)
    with open(of_p, "w", newline="") as of:

        csv_writer_object = csv.writer(of)
        header = ["county_fips", "proportion_stayed_at_home"]
        csv_writer_object.writerow(header)

        for county_fips, proportion_at_home in county_fips_to_proportion_completely_at_home.items():
            data_row = [county_fips, proportion_at_home]
            csv_writer_object.writerow(data_row)

def merge_census_and_distancing_data(date="04-23"):
    """ Write census and social distancing data to a single file.

    Input files
    -----------
    ../data/safegraph_aggregated_{}_by_county.csv
    ../data/census_gender_by_county.csv

    Output file
    ---------
    ../data/census_and_safegraph_data_merged.csv
    """

    # Load census and distancing data.
    SAFEGRAPH_DATA_P = "../data/safegraph_aggregated_{}_by_county.csv".format(date)
    CENSUS_DATA_P = "../data/census_gender_by_county.csv"
    safegraph_dataframe = pd.read_csv(SAFEGRAPH_DATA_P)
    census_dataframe = pd.read_csv(CENSUS_DATA_P)
   
    # (0-pad county fips code to 5 digits e.g. 1001 -> 01001)
    safegraph_dataframe["county_fips"] = [str(unpadded_fips).zfill(5) for 
            unpadded_fips in safegraph_dataframe["county_fips"]]
    census_dataframe["county_fips"] = [str(unpadded_fips).zfill(5) for 
            unpadded_fips in census_dataframe["county_fips"]]
    
    # Merge census and distancing data.
    county_fips_to_prop_stayed_home = {fips : stay_home for fips, stay_home in 
            zip(safegraph_dataframe.county_fips, 
                safegraph_dataframe.proportion_stayed_at_home)
        }
    county_fips_to_prop_male = {fips : male for fips, male in 
            zip(census_dataframe.county_fips, 
                census_dataframe.prop_male)
        }


    # Write the merged dataframe.
    of_p = "../data/census_and_safegraph_data_merged.csv"
    with open(of_p, "w", newline="") as of:

        csv_writer_object = csv.writer(of)
        header = ["county_fips", "proportion_male", "proportion_stayed_at_home"]
        csv_writer_object.writerow(header)

        for county_fips, prop_male in county_fips_to_prop_male.items():

            try:
                prop_stayed_at_home = county_fips_to_prop_stayed_home[county_fips]
                data_row = [county_fips, prop_male, prop_stayed_at_home]
                csv_writer_object.writerow(data_row)
    
            except KeyError: # non-shared county, this is expected. 
                pass

def analyze_merged_data():

    # load merged data.
    df = pd.read_csv("../data/census_and_safegraph_data_merged.csv")

    # correlate gender with staying at home
    rho, p = spearmanr(df.proportion_male, df.proportion_stayed_at_home)
    print("correlation of male with stay at home, rho = {:.4f} p = {:.4f}".format(rho, p))


def preprocess_covid_deaths_data():

    # Load table of COVID deaths.
    covid_data_p = "../data/Provisional_COVID-19_Death_Counts_by_Sex__Age__and_State.csv"
    covid_raw_dataframe = pd.read_csv(covid_data_p)

    # Map states to male and female deaths.
    state_to_male_deaths = {}
    state_to_female_deaths = {}
    data_colnames = ["State", "Age group", "COVID-19 Deaths"]
    for state, age_group, covid_deaths in (
        covid_raw_dataframe[data_colnames]).values:
    
        # age group "Female, all ages"   "Male, all ages"
        if age_group == "Female, all ages":
            state_to_female_deaths[state] = covid_deaths
        elif age_group == "Male, all ages":
            state_to_male_deaths[state] = covid_deaths

    # Write table:
    # State   Male Deaths   Female Deaths    Male/Total
    of_p = "../data/covid_deaths_by_state_and_gender_preprocessed.csv"
    with open(of_p, "w", newline="") as of:

        # Write header.
        w = csv.writer(of)
        header = ["state", "male_deaths", "female_deaths", "male_deaths_over_total_deaths"]
        w.writerow(header)

        # Write data rows.
        for state, male_deaths in state_to_male_deaths.items():
            
            female_deaths = state_to_female_deaths[state]
            
            # special handling is required in case of 0 or nan death counts.
            VALID_MALE_DEATHS = not np.isnan(male_deaths) and male_deaths > 0
            VALID_FEMALE_DEATHS = not np.isnan(female_deaths) and female_deaths > 0

            if VALID_MALE_DEATHS and VALID_FEMALE_DEATHS:
                prop_male_deaths = male_deaths / (male_deaths + female_deaths)
            else:
                prop_male_deaths = np.nan

            data_row = [state, male_deaths, female_deaths, prop_male_deaths]
            w.writerow(data_row)


def aggregate_distancing_by_state():

    # Load social distancing data.
    DATA_P = "../data/census_and_safegraph_data_merged.csv"
    df = pd.read_csv(DATA_P)

    # Load a mapping of state fips codes to state names.
    FIPS_CODES_P = "../data/state_fips_codes.csv"
    fips_df = pd.read_csv(FIPS_CODES_P)
    fips_code_to_state_name = {}
    for state, fips in fips_df.values:
        fips_code_to_state_name[str(fips)] = state
    
    # Add a column to the social distancing data for state name.
    state_fips_codes = [str(county_fips)[:2] for county_fips in df.county_fips]
    state_names = [fips_code_to_state_name[fips] for fips in state_fips_codes]
    df["state"] = state_names

    keep_colnames = ["state", "proportion_male", "proportion_stayed_at_home"]
    df = df[keep_colnames]

    # Aggregate gender (prop male) and distancing by state.
    df = df.groupby("state").mean()
    
    # Save results.
    of_p = "../data/census_and_safegraph_aggregated_by_state.csv"
    df.to_csv(of_p)

def merge_covid_and_distancing_data():

    # Load both datasets.


if __name__ == "__main__":
    #preprocess_census_data()

    """
    days = ["01", "07", "13", "23", "24"]
    for day in days:
        date = "04-{}".format(day)
        preprocess_distancing_data(date)
        merge_census_and_distancing_data(date)
        analyze_merged_data()
    """

    #preprocess_covid_deaths_data()
    #aggregate_distancing_by_state()
