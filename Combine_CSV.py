import pandas as pd
import glob
import os


os.chdir(r"Your Path")


csv_files = glob.glob("orders_*.csv")


df = pd.concat([pd.read_csv(f) for f in csv_files])
df.to_csv("combined_orders.csv", index=False)

print(" All files merged into combined_orders.csv")
