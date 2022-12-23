import nhanes_loader
import pandas
import warnings

warnings.simplefilter(action='ignore', category=pandas.errors.PerformanceWarning)


test_list = ["THYROD", "CBC"]
csv_file = "NHANESLoader\\data\\data.csv"

data_directory = "NHANESLoader\\data"

nhanes_loader.download_nhanes(["Laboratory"], ["2017"], data_directory)

nhanes_loader.nhanes_merger_numpy(data_directory, csv_file, test_list, all_attributes=True)  # Scrape and creates CSV
# df = nhanes_loader.load_csv(csv_file, min_age=18, max_age=25)  # Load the created CSV file into a dataframe
