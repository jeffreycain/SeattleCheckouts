import csv
import pandas as pd
csvin = pd.read_csv('C:/Users/b.fisher/Downloads/seattle-checkouts-by-title/checkouts-by-title.csv')

csvin.to_csv('C:/Users/b.fisher/Documents/K-State/18Fall/CIS490/TermProject/checkouts-by-title.tsv',sep='\t', quoting=csv.QUOTE_NONE)