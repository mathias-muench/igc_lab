#!/usr/bin/env python
# coding: utf-8


import sys
from scraped import ScrapedFlight
import pdflight
from joblib import Parallel, delayed


flights = Parallel(n_jobs=-1)(
    delayed(ScrapedFlight.create_from_file)(i) for i in sys.argv
)


(md, fl, th) = pdflight.flights_to_dataframes(flight for flight in flights if flight.valid)
md.to_csv("md.tsv", sep="\t")
