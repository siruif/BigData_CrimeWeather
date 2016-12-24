
import pandas as pd


df = pd.read_csv("crimeData.csv")

df["Block"]= df['Block'].str.replace(',','')

df["Description"]= df['Description'].str.replace(',','')

df["Location Description"]= df['Location Description'].str.replace(',','')

df.to_csv('crimeData.csv')

print("Done replacing commas.")