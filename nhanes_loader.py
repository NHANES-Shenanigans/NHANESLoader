import bisect
import numpy
import os
import pandas
import random
import requests
import shutil
from bs4 import BeautifulSoup
from urllib.parse import urlparse


def get_url_base(url):
    parsed_uri = urlparse(url)
    return f"{parsed_uri.scheme}://{parsed_uri.netloc}"


def augment_url_with_site(url, site, prefix="http"):
    if prefix not in url:
        url = get_url_base(site) + ("/" if url[0] != "/" else "") + url
    return url


def get_links(url, extensions):
    response = requests.get(url)
    contents = response.content

    soup = BeautifulSoup(contents, "html.parser")
    links = []
    for link in soup.findAll("a"):
        try:
            for extension in extensions:
                if extension in link["href"]:
                    links.append(link["href"])
        except KeyError:
            pass
    return links


def remove_prefix(path, prefix):
    if path.startswith(prefix):
        return path[len(prefix):]
    return path


def list_links(url, extensions=None):
    if extensions is None:
        extensions = [""]

    for link in get_links(url, extensions):
        print(f"{link}")


def download_links(links, path_removal, output_directory):
    for count, link in enumerate(links, start=1):
        link_processed = remove_prefix(link, path_removal)
        nhanes_dir = os.path.dirname(link_processed)
        new_directory = output_directory + "\\" + nhanes_dir
        file_name = new_directory + "\\" + os.path.basename(link_processed)
        print(f"File {count}/{len(links)} | Source: {link} | Destination: \"{file_name}\"")

        os.makedirs(new_directory, exist_ok=True)
        if not os.path.isfile(file_name):
            response = requests.get(link, stream=True)
            with open(file_name, "wb") as file:
                shutil.copyfileobj(response.raw, file, length=16*1024*1024)
        else:
            print("Skipped file (already downloaded)")


def download_url_links(url, extensions, path_removal, output_dir):
    links = get_links(url, extensions)
    links = [augment_url_with_site(link, url) for link in links]
    download_links(links, path_removal, output_dir)


def download_nhanes(components, years, destination, default_url=True):
    for year in years:
        for component in components:
            print("=" * 100)
            if default_url:
                url = f"https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component={component}&CycleBeginYear={year}"
                removal = "https://wwwn.cdc.gov/Nchs/"
            else:
                url = "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + component + ".aspx?BeginYear=" + year
                removal = "https://wwwn.cdc.gov/nchs/data/"
            print(f"{year} : {component} => {url}")

            links = get_links(url, [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"])
            links_htm = get_links(url, [".htm"])
            for link in links_htm:
                pre, ext = os.path.splitext(link)
                links_xpt = pre + ".XPT"
                links_dat = pre + ".dat"
                links_sas = pre + ".sas"
                if (links_xpt in links) or (links_dat in links) or (links_sas in links):
                    links.append(link)

            links = [augment_url_with_site(link, url) for link in links]
            random.shuffle(links)
            download_links(links, removal, destination)


# def download_all_nhanes():
#     download_nhanes(["Demographics", "Dietary", "Examination", "Laboratory", "Questionnaire", "Non-Public"],
#                     ["1999", "2001", "2003", "2005", "2007", "2009", "2011", "2013", "2015", "2017"])
#     download_nhanes(["Questionnaires", "labmethods", "Manuals", "Documents", "overview", "releasenotes", "overviewlab",
#                      "overviewquex", "overviewexam"],
#                     ["1999", "2001", "2003", "2005", "2007", "2009", "2011", "2013", "2015", "2017"], default_url=False)


def browse_directory_tables(directory, extensions=None):
    if extensions is None:
        extensions = [""]

    file_names = []
    for root, directories, files in os.walk(directory):
        for file in files:
            for extension in extensions:
                if extension in file:
                    file_names.append(os.path.join(root, file))
    return file_names


def count_elements(directory, attributes=None, all_elements=False):
    if attributes is None:
        attributes = [""]

    sequence_numbers = []
    columns = []
    count = 0
    total_size = 0

    not_included = []
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all_elements:
                    for attribute in attributes:
                        if attribute in file:
                            found = True
                if (not found) and (not all_elements):
                    not_included.append(file)
                else:
                    file_name = os.path.join(root, file)
                    print(f"Opening file {file_name}")
                    data = pandas.read_sas(file_name)
                    if "SEQN" in data:
                        total_size = total_size + os.path.getsize(file_name)
                        count = count + 1
                        for column in list(data):
                            columns.append(column)
                        for sequence_number in data["SEQN"].values:
                            sequence_numbers.append(sequence_number)
                    else:
                        not_included.append(file)

    print("=" * 10, "Not included", "=" * 10)
    print(f"{not_included}")
    columns = list(dict.fromkeys(columns))
    sequence_numbers = list(dict.fromkeys(sequence_numbers))
    columns.sort()
    sequence_numbers.sort()

    return sequence_numbers, columns, total_size, count


def get_elements(sequence_numbers, columns, directory, attributes, num_files=0, all_elements=False):
    total_sequences = len(sequence_numbers)
    total_columns = len(columns)
    data = numpy.empty((total_sequences, total_columns))
    data[:] = numpy.NaN

    print("Loading Files")
    count = 1
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all_elements:
                    for a in attributes:
                        if a in file:
                            found = True
                if all_elements or found:
                    file_name = os.path.join(root, file)
                    df = pandas.read_sas(file_name)
                    columns = list(dict.fromkeys(list(df)))
                    if "SEQN" in columns:
                        print(f"Reading file {count}/{num_files} {file_name}")
                        count = count + 1
                        for index, row in df.iterrows():
                            sequence_index = bisect.bisect_left(sequence_numbers, row["SEQN"])
                            for column in columns:
                                try:
                                    column_index = bisect.bisect_left(columns, column)
                                    data[sequence_index][column_index] = row[column]
                                except ValueError:
                                    print(f"Error {row[column]} {type(row[column])} {column} {file_name}")
    return data


def numpy_to_csv(data, columns, destination):
    header = ""
    for column in columns:
        header = header + column + ", "
    print(f"header: {header}")
    numpy.savetxt(destination, data, header=header, delimiter=", ", comments="")


def numpy_to_pandas(data, columns):
    dataframe = pandas.DataFrame(data, columns=columns)
    return dataframe


def nhanes_merger_numpy(directory, destination, attributes=None, all_attributes=False):
    if attributes is None:
        attributes = [""]

    sequence_numbers, columns, total_size, num_files = count_elements(directory, attributes, all_attributes)
    total_sequences = len(sequence_numbers)
    total_columns = len(columns)
    print(
        f"===> Database filtering info:  ( nb Part {total_sequences}) (nb Columns {total_columns}) (total file size ("
        f"MBs) {total_size / 1024 / 1024}) (nb Files) {num_files}")
    data = get_elements(sequence_numbers, columns, directory, attributes, num_files, all_attributes)
    dataframe = numpy_to_pandas(data, columns)
    dataframe.to_csv(destination)
    return dataframe


def load_csv(name, min_age=-1, max_age=200):
    dataframe = pandas.read_csv(name, low_memory=False)
    if "RIDAGEYR" not in dataframe:
        return dataframe

    index = [x and y for x, y in zip((dataframe["RIDAGEYR"] >= min_age), (dataframe["RIDAGEYR"] <= max_age))]
    return dataframe[index]


def keep_non_null(dataframe, column):
    x = (~dataframe[column].isnull())
    return dataframe[x]


def keep_equal(dataframe, column, value):
    x = (dataframe[column] == value)
    return dataframe[x]


def keep_different(dataframe, column, value):
    x = (dataframe[column] != value)
    return dataframe[x]


def keep_greater_than(dataframe, column, value):
    x = (dataframe[column] > value)
    return dataframe[x]


def keep_greater_equal(dataframe, column, value):
    x = (dataframe[column] >= value)
    return dataframe[x]


def keep_lower_than(dataframe, column, value):
    x = (dataframe[column] < value)
    return dataframe[x]


def keep_lower_equal(dataframe, column, value):
    x = (dataframe[column] <= value)
    return dataframe[x]


def keep_columns(dataframe, columns):
    return dataframe[columns]
