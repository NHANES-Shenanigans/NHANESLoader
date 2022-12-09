import bisect
import os
import random
from urllib.parse import urlparse

import numpy as np
import pandas
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_url_base(url):
    parsed_uri = urlparse(url)
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
    return result


def augment_url_with_site(url, site, pref="http"):
    if pref not in url:
        url = get_url_base(site) + ("/" if url[0] != "/" else "") + url
    return url


def get_links(url, extensions):
    response = requests.get(url)
    contents = response.content

    soup = BeautifulSoup(contents, "lxml")
    links = []
    for link in soup.findAll('a'):
        try:
            for extension in extensions:
                if extension in link['href']:
                    links.append(link['href'])
        except KeyError:
            pass
    return links


def remove_prefix(target, prefix):
    if target.startswith(prefix):
        return target[len(prefix):]
    return target


def list_links(url, extensions=[""]):
    for link in get_links(url, extensions):
        print(link)


def go_through_directory(path_removal, link, output_directory):
    link = link.remove_prefix(path_removal)
    # while "\\" in link:
    # os.makedirs(path, exist_ok=True)


def download_links(links, path_removal, output_directory):
    count = 1
    for link in links:
        link_processed = remove_prefix(link, path_removal)
        directory = os.path.dirname(link_processed)
        new_directory = output_directory + "\\" + directory
        file_path = new_directory + "\\" + os.path.basename(link_processed)
        print(link)
        print("file ", count, " / ", len(links), " (" + file_path + ")")
        count = count + 1
        try:
            os.makedirs(new_directory, exist_ok=True)
            if not os.path.isfile(file_path):
                response = requests.get(link, stream=True)
                with open(file_path, "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                    handle.close()
            else:
                print("Skipped file as already created")
        except BaseException:
            print("!!! PROBLEM Creating ", file_path)


def download_url_links(url, extensions, path_removal, output_dir):
    links = get_links(url, extensions)
    links = [augment_url_with_site(x, url) for x in links]
    download_links(links, path_removal, output_dir)


def download_nhanes(components, years, type=1):
    #  prefix="https://wwwn.cdc.gov"
    for year in years:
        for component in components:
            print()
            print(
                "======================================================================================================================")
            if (type == 1):
                url = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=" + \
                      component + "&CycleBeginYear=" + year
                removal = "https://wwwn.cdc.gov/Nchs/"
            else:
                url = "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + component + ".aspx?BeginYear=" + year
                removal = "https://wwwn.cdc.gov/nchs/data/"
            print(year, ":", component, "       =>", url)
            print("")
            extensions = [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"]
            links = get_links(
                url, [
                    ".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"])
            links_htm = get_links(url, [".htm"])
            for link in links_htm:
                prefix, extension = os.path.splitext(link)
                link_xpt = prefix + ".XPT"
                link_dat = prefix + ".dat"
                link_sas = prefix + ".sas"
                if (link_xpt in links) or (link_dat in links) or (link_sas in links):
                    links.append(link)
            # links = [prefix + sub for sub in links]
            links = [augment_url_with_site(link, url) for link in links]
            random.shuffle(links)
            download_links(links, removal, "C:\\Tmp\\")


def download_nhanes_b(components, years):
    for year in years:
        for component in components:
            download_links(
                "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" +
                component +
                ".aspx?BeginYear=" +
                year,
                [
                    ".XPT",
                    ".dat",
                    ".sas",
                    ".txt",
                    ".pdf",
                    ".doc"],
                "https://wwwn.cdc.gov/Nchs/data/",
                "C:\\Tmp\\")


def download_all_nhanes():
    # downloadNhanes(["Demographics"],["2017"]);
    # listLinks("https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=Demographics&CycleBeginYear=2017")

    download_nhanes(["Demographics",
                     "Dietary",
                     "Examination",
                     "Laboratory",
                     "Questionnaire",
                     "Non-Public"],
                    ["2017",
                     "2015",
                     "2013",
                     "2011",
                     "2009",
                     "2007",
                     "2005",
                     "2003",
                     "2001",
                     "1999"])
    download_nhanes(["Questionnaires",
                     "labmethods",
                     "Manuals",
                     "Documents",
                     "overview",
                     "releasenotes",
                     "overviewlab",
                     "overviewquex",
                     "overviewexam"],
                    ["2017",
                     "2015",
                     "2013",
                     "2011",
                     "2009",
                     "2007",
                     "2005",
                     "2003",
                     "2001",
                     "1999"],
                    type=2)

    # downloadNhanes(["Demographics","Dietary","Examination","Questionnaire","Non-Public"],["1999"]);
    # downloadNhanesB(["Questionnaires","LabMethods","Manuals","Documents","DocContents","OverviewLab","OverviewQuex","OverviewExam"],["1999"]);
    # downloadLinks("https://wwwn.cdc.gov/nchs/nhanes/nhanes3/DataFiles.aspx", [".xpt",".dat",".sas",".txt",".pdf",".doc"], "https://wwwn.cdc.gov/nchs/data", "E:\Ben\Research\Datasets\Life Science\\")
    # downloadLinks("https://www.cdc.gov/nchs/nhanes/nh3data.htm", [".xpt",".dat",".sas",".txt",".pdf"], "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/nhanes","E:\Ben\Research\Datasets\Life Science\\")


def browse_directory_tables(directory, extensions=[""]):
    file_names = []
    for root, directories, files in os.walk(directory):
        for file in files:
            for extension in extensions:
                if extension in file:
                    file_names.append(os.path.join(root, file))
    return file_names


def count_elements(directory, attributes=[""], all_attributes=False):
    sequence_numbers = []
    columns = []
    count = 0
    total_size = 0

    not_included = []
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all_attributes:
                    for attribute in attributes:
                        if attribute in file:
                            found = True
                if (not found) and (not all_attributes):
                    not_included.append(file)
                else:
                    file_name = os.path.join(root, file)
                    print('Opening file', file_name)
                    dataframe = pandas.read_sas(file_name)
                    if 'SEQN' in dataframe:
                        total_size = total_size + os.path.getsize(file_name)
                        count = count + 1
                        for column in list(dataframe):
                            columns.append(column)
                        for sequence_number in dataframe['SEQN'].values:
                            sequence_numbers.append(sequence_number)
                    else:
                        not_included.append(file)
    print("========================= Not included: ====================================")
    print(not_included)
    columns = list(dict.fromkeys(columns))
    sequence_numbers = list(dict.fromkeys(sequence_numbers))
    columns.sort()
    sequence_numbers.sort()

    return sequence_numbers, columns, total_size, count


def get_elements(sequence_numbers, columns, directory, attributes, num_files=0, all_attributes=False):
    total_sequence_numbers = len(sequence_numbers)
    total_columns = len(columns)
    data = np.empty((total_sequence_numbers, total_columns))
    data[:] = np.NaN
    print("Loading Files")
    count = 0
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if (not all_attributes):
                    for attribute in attributes:
                        if attribute in file:
                            found = True
                if all_attributes or found:
                    file_name = os.path.join(root, file)
                    dataframe = pandas.read_sas(file_name)
                    all_columns = list(dict.fromkeys(list(dataframe)))
                    if 'SEQN' in all_columns:
                        print('Reading file  ', count, "/", num_files, file_name)
                        count = count + 1
                        for index, row in dataframe.iterrows():
                            sequence_number_index = bisect.bisect_left(sequence_numbers, row['SEQN'])
                            for column in all_columns:
                                try:
                                    column_index = bisect.bisect_left(columns, column)
                                    data[sequence_number_index][column_index] = row[column]
                                except ValueError:
                                    # print('Error:',row[column],type(row[column]), column, file_name)
                                    pass
    return data


def np_to_csv(data, columns, destination='e:/nhanesTestVeryFast3.csv'):
    header = ''
    for column in columns:
        header = header + column + ', '
    print("header")
    print(header)
    np.savetxt(destination, data, header=header, delimiter=', ', comments='')
    pass


def np_to_pandas(data, columns):
    dataframe = pandas.DataFrame(data, columns=columns)
    return dataframe


def nhanes_merger_numpy(directory, attributes=[""], destination='e:/nhanesF.csv', all_attributes=False):
    sequence_numbers, columns, total_size, num_files = count_elements(directory, attributes, all_attributes)
    total_sequence_numbers = len(sequence_numbers)
    total_columns = len(columns)
    print(
        "===> Database filtering info:  ( nb Part",
        total_sequence_numbers,
        ') (nb Columns',
        total_columns,
        ') (total file size (MBs)',
        total_size / 1024 / 1024,
        ') (nb Files)',
        num_files)
    data = get_elements(sequence_numbers, columns, directory, attributes, num_files, all_attributes)
    # npToCSV(data,columns,dest)
    dataframe = np_to_pandas(data, columns)
    dataframe.to_csv(destination)
    return dataframe


def load_csv(name, min_age=-1, max_age=200):
    dataframe = pandas.read_csv(name, low_memory=False)
    if 'RIDAGEYR' in dataframe:
        x = [
            x and y for x,
            y in zip(
                (dataframe['RIDAGEYR'] >= min_age),
                (dataframe['RIDAGEYR'] <= max_age))]
        return dataframe[x]
    else:
        return dataframe


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
