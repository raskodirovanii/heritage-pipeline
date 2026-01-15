import pandas as pd

df = pd.read_csv("data/processed/exhibits.csv")

print("\nТОП музеев:")
print(df["museum_name"].value_counts().head(10))

print("\nТОП периодов:")
print(df["periodStr"].value_counts().head(10))

print("\nТОП типологий:")
print(df["typology_name"].value_counts().head(10))
