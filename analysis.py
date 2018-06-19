import argparse
import itertools
from tqdm import tqdm
from os import path 
from lib.dataset import Dataset
from lib.pricedata import PriceTimeSeriesNaive, PriceBucketNaive
from lib.important_times import BEFORE_END_EPOCH, AFTER_START_EPOCH

def main():
    parser = argparse.ArgumentParser(description="Generate some useful quantile graphs and perhaps a few histograms too")
    parser.add_argument("data_directory", help="the location where we are storing the data.")
    args = parser.parse_args()

    not_enough_data = []

    def perform_analysis(az, insttype, data_before, data_after):
        data_before = PriceTimeSeriesNaive(data_before)
        data_after = PriceTimeSeriesNaive(data_after)

        days_required = 90
        bucket_interval = 350 # bucket interval in seconds
        bucket_before = PriceBucketNaive(data_before.expand_with_interval(bucket_interval))
        bucket_after = PriceBucketNaive(data_after.expand_with_interval(bucket_interval))

        if len(bucket_after.by_ts) == 0 or len(bucket_before.by_ts) == 0:
            return 

        SECONDS_IN_DAY = 3600 * 24
        SIZE_REQUIREMENT = SECONDS_IN_DAY / bucket_interval * days_required
        if bucket_before.size() < SECONDS_IN_DAY or bucket_after.size() < SECONDS_IN_DAY:
            print("\tnot enough data for region")
        bucket_before = bucket_before.shrink_to_size(min_size)
        bucket_after = bucket_after.shrink_to_size(min_size)

        mean = bucket_before.getAverage()
        median = bucket_before.getQuantile(0.5)["price"]

        print("\tprice window duration: %f" % ((bucket_before.by_ts[-1]["timestamp"] - bucket_before.by_ts[0]["timestamp"]) / (3600 * 24)))
        print("\tmean before/after: %f/%f" % (float(bucket_before.getAverage()), float(bucket_after.getAverage())))
        print("\tmedian before/after: %f/%f" % (float(bucket_after.getQuantile(0.5)["price"]), bucket_after.getQuantile(0.5)["price"]))
        
        total_cost_before = sum(r["price"] for r in bucket_before.by_ts)
        print("\ttotal/avg cost before: %f/%f" % (
            total_cost_before,
            total_cost_before / float(len(bucket_before.by_ts))
        ))

        total_cost_after = sum(r["price"] for r in bucket_after.by_ts)
        print("\ttotal/avg cost after: %f/%f" % (
            total_cost_after,
            total_cost_after / float(len(bucket_before.by_ts))
        ))

        print("\ttotal/avg price difference: %f/%f" % (
            total_cost_after - total_cost_before,
            (total_cost_after - total_cost_before) / float(len(bucket_before.by_ts))
        ))

        # return the result object containing aggregates of the values we computed
        return {
            "key": (az, insttype),
            "'popularity'": len(data_before.data) + len(data_after.data),
            "total_cost_before": total_cost_before,
            "avg_cost_before": total_cost_before / float(len(bucket_before.by_ts)),
            "total_cost_after": total_cost_after,
            "avg_cost_after": total_cost_after / float(len(bucket_before.by_ts))
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
            # count += 1
            # if count > 100:
            #     break 

    results = list(filter(lambda x: x is not None, results))
    results.sort(key=lambda r: r["'popularity'"])

    print("Information filtered by instance popularity")
    def aggregate_dictionary(dicts, aggfunc):
        return {key: aggfunc([d[key] for d in dicts]) for key in sorted(dicts[0].keys()) if type(dicts[0][key]) == int or type(dicts[0][key]) == float}

    def aggmedian(values):
        return sorted(values)[len(values)//2]

    def aggaverage(values):
        return sum(values) / float(len(values))
    
    def print_results(popdict, unpopdict):
        for key in popdict:
            print("\t%s popular/unpopular: %f/%f" % (key, popdict[key], unpopdict[key]))

    popular_inst_types = ["m1.small", "m1.large", "t1.micro", "m1.medium", "c1.xlarge", "c1.medium"]
    results_popular = list(filter(lambda x: x["key"][1] in popular_inst_types, results))
    results_unpopular = list(filter(lambda x: x["key"][1] not in popular_inst_types, results))

    print("results (median, hard coded popular instance types)")
    print("%d popular results found" % (len(results_popular)))
    pop_results = aggregate_dictionary(results_popular, aggaverage)
    unpop_results = aggregate_dictionary(results_unpopular, aggaverage)
    print_results(pop_results, unpop_results)

main()