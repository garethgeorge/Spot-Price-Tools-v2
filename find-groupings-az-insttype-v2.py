"""
    reads output of analysis-v2 and tries to find groupings based on which instances
    increased or dropped in price
"""

import json
import itertools
from collections import defaultdict
from tqdm import tqdm 


with open("output.json", "r") as f:
    data = json.load(f)

    data = list(filter(lambda x: x is not None, data))

    # increased = []
    # decreased = []
    # recent_instances_types = ["r4"]

    before_prices_az = {}
    after_prices_az = {}
    before_prices_insttype = {}
    after_prices_insttype = {}

    #
    # grouped by az
    # 
    data.sort(key=lambda x: x["key"][0]) 

    print("grouping by az - showing sums across all instances in the AZ")
    print("%30s %10s %10s %10s" % ("name", "before total", "after total", "delta"))
    for az, results in itertools.groupby(data, key=lambda x: x["key"][0]):
        results = list(results)
        before_prices = [result["total_cost_before"] for result in results]
        after_prices = [result["total_cost_after"] for result in results]

        print("%30s %10.2f %10.2f %10.2f" % (az, sum(before_prices), sum(after_prices), sum(after_prices) - sum(before_prices)))

    #
    # grouped by insttype
    # 
    data.sort(key=lambda x: x["key"][1]) 

    print("\n\ngrouping by insttype - showing sums across all az's offering the insttype")
    print("%30s %10s %10s %10s" % ("name", "before total", "after total", "delta"))
    for az, results in itertools.groupby(data, key=lambda x: x["key"][1]):
        results = list(results)
        before_prices = [result["total_cost_before"] for result in results]
        after_prices = [result["total_cost_after"] for result in results]

        print("%30s %10.2f %10.2f %10.2f" % (az, sum(before_prices), sum(after_prices), sum(after_prices) - sum(before_prices)))