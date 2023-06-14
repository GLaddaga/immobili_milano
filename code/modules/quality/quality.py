import os
import pandas as pd
from ydata_profiling import ProfileReport

def analyze_data_quality():
    datadir = "./data/raw"
    filenames = os.listdir("./data/raw")
    filenames.remove("listings_urls.csv")

    for filename in filenames:
        print(filename)
        data = pd.read_csv(os.path.join(datadir, filename), sep=";")
        profilename = "_".join([filename, "quality"])
        profile = ProfileReport(data, title=profilename)
        profile.to_file("".join(["./data/quality/", profilename, ".html"]))

if __name__ == "__main__":
    analyze_data_quality()
