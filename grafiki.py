# plots.py
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("data/processed/exhibits.csv")

df["museum_name"].value_counts().head(10).plot(kind="bar")
plt.title("Топ музеев по количеству экспонатов")
plt.tight_layout()
plt.show()
