"""
    reads output of analysis-v2 and tries to find groupings based on which instances
    increased or dropped in price
"""

import json
from collections import defaultdict
from tqdm import tqdm 
from numpy import median, percentile

with open("output.json", "r") as f:
    data = json.load(f)

    data = list(filter(lambda x: x is not None, data))

    # increased = []
    # decreased = []
    # recent_instances_types = ["r4"]

    decreased_counts = {}
    increased_counts = {}
    decreased_prices = {}
    increased_prices = {}

    for result in data:
        az, insttype = tuple(result["key"])

        delta = result["total_cost_after"] - result["total_cost_before"]

        if insttype not in decreased_counts:
            decreased_counts[insttype] = 0
            increased_counts[insttype] = 0
            decreased_prices[insttype] = []
            increased_prices[insttype] = []
        
        if delta > 0:
            increased_counts[insttype] += 1
            increased_prices[insttype].append(delta)
        else:
            decreased_counts[insttype] += 1
            decreased_prices[insttype].append(delta)
      
    print("portion of instances that increased in price")
    insttypes_increased = 0
    insttypes_median_increased = 0
    insttypes = 0
    for key in decreased_counts:
        increased = increased_counts[key]
        decreased = decreased_counts[key]
        increased_p = increased_prices[key]
        decreased_p = decreased_prices[key]
        combined_p = increased_p + decreased_p 
        increased_p.sort()
        decreased_p.sort()
        combined_p.sort()

        total = increased + decreased
        print("%s - %d/%d - %f" % (key, increased, increased + decreased, float(increased) / float(total)))
        print("\tmedian decrease: %s median increase: %s median price delta: %s" % (
            str(median(decreased_p)), 
            str(median(increased_p)), 
            str(median(combined_p))
        ))
        if float(increased) > float(total) * 0.5:
            insttypes_increased += 1
        if median(combined_p) > 0:
            insttypes_median_increased += 1
        insttypes += 1
    print("insttypes increased/total insttypes: %d/%d=%f" % ( insttypes_increased, insttypes, float(insttypes_increased)/float(insttypes)))
    print("insttypes median increased/total %d/%d=%f" % ( insttypes_median_increased, insttypes, float(insttypes_median_increased)/float(insttypes)))