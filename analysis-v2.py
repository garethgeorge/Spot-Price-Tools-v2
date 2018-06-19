import argparse
import itertools
import json 
from tqdm import tqdm
from os import path 
from lib.dataset import Dataset
from lib.pricedata import PriceTimeSeriesNaive, PriceBucketNaive
from lib.important_times import BEFORE_END_EPOCH, AFTER_START_EPOCH

def main():
    parser = argparse.ArgumentParser(description="Generate some useful quantile graphs and perhaps a few histograms too")
    parser.add_argument("data_directory", help="the location where we are storing the data.")
    parser.add_argument("output_file", help="the file where we will dump the JSON result aggregate data for analysis")
    args = parser.parse_args()

    not_enough_data = []
    cost_increased_types = []
    cost_decreased_types = []

    def perform_analysis(az, insttype, data_before, data_after):
        data_before = PriceTimeSeriesNaive(data_before)
        data_after = PriceTimeSeriesNaive(data_after)

        days_required = 90
        hours_required = days_required * 24
        
        bucket_interval = 350 # bucket interval in seconds
        bucket_before = PriceBucketNaive(data_before.expand_with_interval(bucket_interval))
        bucket_after = PriceBucketNaive(data_after.expand_with_interval(bucket_interval))

        SECONDS_IN_DAY = 3600 * 24
        BUCKETS_PER_DAY = SECONDS_IN_DAY / bucket_interval
        BUCKETS_PER_HOUR = 3600 / bucket_interval
        SIZE_REQUIREMENT = int(BUCKETS_PER_DAY * days_required)
        if bucket_before.size() < SIZE_REQUIREMENT or bucket_after.size() < SIZE_REQUIREMENT:
            print("\tnot enough data for analysis")
            not_enough_data.append((az, insttype))
            return 
        print("\tanalyzing!")
        # shrink down to exactly the size requirement
        bucket_before = bucket_before.shrink_to_size(SIZE_REQUIREMENT)
        bucket_after = bucket_after.shrink_to_size(SIZE_REQUIREMENT)

        mean = bucket_before.getAverage()
        median = bucket_before.getQuantile(0.5)["price"]
        
        # return the result object containing aggregates of the values we computed
        return {
            "key": (az, insttype),
            "interval_days": days_required, # how many days we are looking at
            "total_cost_before": bucket_before.getAverage() * hours_required,
            "total_cost_after": bucket_after.getAverage() * hours_required,
            "mean_price_before": bucket_before.getAverage(),
            "mean_price_after": bucket_after.getAverage(),
            "median_price_before": bucket_before.getQuantile(0.5),
            "median_price_after": bucket_after.getQuantile(0.5),
        }

    def transform_query_results(results):
        return ({"timestamp": result[0], "price": result[1]} for result in results)

    results = []

    with Dataset(args.data_directory) as dataset:
        count = 0 
        for az, insttype in dataset.get_databases(): # misleading function name            
            conn = dataset.open(az, insttype)
            
            # compute the most recent timestamp so we can determine a good
            # duration for the intervals we will be examining
            c = conn.cursor()
            c.execute("SELECT MAX(timestamp) AS timestamp FROM prices")
            most_recent_ts = c.fetchone()[0]
            interval_duration = most_recent_ts - AFTER_START_EPOCH

            if interval_duration < 0: continue 
            
            # fetch the data for the before window
            c = conn.cursor()
            c.execute("SELECT timestamp, price FROM prices  "
                " WHERE timestamp > %d AND timestamp < %d ORDER BY timestamp" 
                % (BEFORE_END_EPOCH - interval_duration, BEFORE_END_EPOCH))
            data_before = list(transform_query_results(c.fetchall()))
            c = conn.cursor()
            c.execute("SELECT timestamp, price FROM prices " +
                " WHERE timestamp > %d ORDER BY timestamp " % (AFTER_START_EPOCH))
            data_after = list(transform_query_results(c.fetchall()))

            if len(data_before) == 0 or len(data_after) == 0: continue

            print("analyzing data for %s - %s" % (az, insttype))

            results.append(
                perform_analysis(az, insttype, data_before, data_after)
            )

    results = list(filter(lambda x: x is not None, results))
    results.sort(key=lambda r: r["key"])

    with open(args.output_file, "w") as f:
        json.dump(results, f)

main()