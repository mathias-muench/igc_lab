#!/usr/bin/env python
# coding: utf-8


import glob
from igc_lib.igc_lib import Flight
import pdflight
from joblib import Parallel, delayed


flights = Parallel(n_jobs=-1)(
    delayed(Flight.create_from_file)(i) for i in glob.glob("SSM2023/*.igc")
)


(md, fl, th) = pdflight.flights_to_dataframes(flights)
md.to_csv("md.tsv", sep="\t")
