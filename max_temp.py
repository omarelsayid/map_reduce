from mrjob.job import MRJob
import csv
import io

# ======================================================
# PART 1: MAPREDUCE JOB
# ======================================================

class MaxTemperature(MRJob):

    def mapper(self, _, line):
        reader = csv.reader(io.StringIO(line))
        row = next(reader, None)

        if not row:
            return

        # Skip CSV header
        if row[0] == "Formatted Date":
            return

        try:
            date = row[0]
            temp = float(row[3])      # Temperature (C)
            year = date[:4]
            yield year, temp
        except:
            pass


    def reducer(self, year, temps):
        yield year, max(temps)

# ======================================================
# PART 2: ANALYTICS + VISUALIZATION
# ======================================================

def run_analytics():
    import pandas as pd
    import matplotlib.pyplot as plt

    print("\n Loading MRJob Results from output.txt")

    file = "output.txt"

    df = pd.read_csv(file, sep="\t", names=["Year", "Max_Temperature_C"])

    print("\n📊 Statistical Summary")
    print(df.describe())

    # -------- REMOVE NaN FROM TREND ----------
    df["Yearly_Trend"] = df["Max_Temperature_C"].diff()
    df["Yearly_Trend"] = df["Yearly_Trend"].fillna(0)  

    print("\n📈 Yearly Temperature Trends")
    print(df)

    # -------- PLOT ----------
    plt.figure(figsize=(10, 5))
    plt.plot(df["Year"], df["Max_Temperature_C"])
    plt.title("Yearly Maximum Temperature Trend")
    plt.xlabel("Year")
    plt.ylabel("Temperature (°C)")
    plt.grid(True)
    plt.show()

# ======================================================
# PART 3: MAIN EXECUTION
# ======================================================

if _name_ == '_main_':

    # Run MapReduce
    MaxTemperature.run()

    # Run analytics after job
    run_analytics()
