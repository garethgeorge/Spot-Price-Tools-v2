import argparse
import itertools
from os import path 
from lib.dataset import Dataset
from lib.pricedata import PriceTimeSeriesNaive, PriceBucketNaive
import matplotlib.pyplot as plt
from lib.important_times import BEFORE_END_EPOCH, AFTER_START_EPOCH

def main():
    parser = argparse.ArgumentParser(description="Generate some useful quantile graphs and perhaps a few histograms too")
    parser.add_argument("data_directory", help="the location where we are storing the data.")
    parser.add_argument("output_directory", help="the location where we will want to output the graphs.")
    args = parser.parse_args()

    def make_graph(az, insttype, data_before, data_after, interval, axes):
        print("building a graph for az: %s insttype: %s" % (az, insttype))
        print("\twe have %d datapoints from before, and %d datapoints from after" % (len(data_before), len(data_after)))

        # create the price data models
        data_before = PriceTimeSeriesNaive(data_before)
        data_after = PriceTimeSeriesNaive(data_after)

        expanded_before = list(data_before.expand_with_interval(350))
        expanded_after = list(data_after.expand_with_interval(350))

        print("\tbefore expanded length: %d" % len(expanded_before))
        print("\tafter expanded length: %d" % len(expanded_after))

        buckets_before = [PriceBucketNaive(list(bucket)) for idx, bucket in itertools.groupby(expanded_before, lambda x: x["timestamp"] // interval)]
        buckets_after = [PriceBucketNaive(list(bucket)) for idx, bucket in itertools.groupby(expanded_after, lambda x: x["timestamp"] // interval)]
        expanded_before, expanded_after = None, None # we null them so that their memory can be collected if need be

        print("\tbuckets before length: %d" % len(buckets_before))
        print("\tbuckets after length: %d" % len(buckets_after))
        
        min_len = min(len(buckets_before), len(buckets_after))
        buckets_before = buckets_before[-min_len:]
        buckets_after = buckets_after[-min_len:]
        
        series_avg_before = [bucket.getAverage() for bucket in buckets_before]
        series_avg_after = [bucket.getAverage() for bucket in buckets_after]
        series_p50_before = [bucket.getQuantile(0.5)["price"] for bucket in buckets_before]
        series_p50_after = [bucket.getQuantile(0.5)["price"] for bucket in buckets_after]
        series_p66_before = [bucket.getQuantile(0.66)["price"] for bucket in buckets_before]
        series_p66_after = [bucket.getQuantile(0.66)["price"] for bucket in buckets_after]
        series_p95_before = [bucket.getQuantile(0.95)["price"] for bucket in buckets_before]
        series_p95_after = [bucket.getQuantile(0.95)["price"] for bucket in buckets_after]

        # f = plt.figure(figsize=(5, 10))

        xvalues = list(range(-min_len, 0))
        axes[0].set_title("%s-%s averages\nduration: %d" % (az, insttype, interval / 3600))
        axes[0].set_ylabel("Price $")
        axes[0].plot(xvalues, series_avg_before, label="before")
        axes[0].plot(xvalues, series_avg_after, label="after")
        
        axes[1].set_title("%s-%s q.5\nduration: %d" % (az, insttype, interval / 3600))
        axes[1].set_ylabel("Price $")
        axes[1].plot(xvalues, series_p50_before, label="before")
        axes[1].plot(xvalues, series_p50_after, label="after")

        axes[2].set_title("%s-%s q.66\nduration: %d" % (az, insttype, interval / 3600))
        axes[2].set_ylabel("Price $")
        axes[2].plot(xvalues, series_p66_before, label="before")
        axes[2].plot(xvalues, series_p66_after, label="after")

        axes[3].set_title("%s-%s q.95\nduration: %d" % (az, insttype, interval / 3600))
        axes[3].set_ylabel("Price $")
        axes[3].plot(xvalues, series_p95_before, label="before")
        axes[3].plot(xvalues, series_p95_after, label="after")
        
        # plt.legend()
        # plt.tight_layout()
        # f.savefig(path.join(args.output_directory, "%s_%s_interval%d.png" % (az, insttype, interval / 3600)), bbox_inches="tight", dpi=150)
        # plt.close()

    def transform_query_results(results):
        return ({"timestamp": result[0], "price": result[1]} for result in results)
            

    with Dataset(args.data_directory) as dataset:
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

            print("producing a graph for %s - %s" % (az, insttype))

            f, axes = plt.subplots(4, 4, figsize=(24, 12))
            axes = axes.flatten()

            make_graph(az, insttype, list(data_before), list(data_after), 3600, axes[0::4])
            make_graph(az, insttype, list(data_before), list(data_after), 8 * 3600, axes[1::4])
            make_graph(az, insttype, list(data_before), list(data_after), 24 * 3600, axes[2::4])
            make_graph(az, insttype, list(data_before), list(data_after), 3 * 24 * 3600, axes[3::4])

            plt.legend()
            plt.tight_layout()
            f.savefig(path.join(args.output_directory, "%s_%s.png" % (az, insttype)), bbox_inches="tight", dpi=150)
            plt.close()

main()
