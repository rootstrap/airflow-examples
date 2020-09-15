#!/usr/bin/python3

import pandas as pd
import sys

pd.read_csv(sys.argv[1]).to_parquet(sys.argv[2])

