"""
    reads output of analysis-v2 and tries to find groupings based on which instances
    increased or dropped in price
"""

import json
from collections import defaultdict
from tqdm import tqdm 

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

        if az not in decreased_counts:
            decreased_counts[az] = 0
            increased_counts[az] = 0
            decreased_prices[az] = []
            increased_prices[az] = []
        
        if delta > 0:
            increased_counts[az] += 1
            increased_prices[az].append(delta)
        else:
            decreased_counts[az] += 1
            decreased_prices[az].append(delta)
    print("portion of instances that increased in price")
    azs_increased = 0
    azs = 0
    azs_median_increased = 0
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
        print("\tmedian decrease: %f median increase: %f median price: %f" % (decreased_p[len(decreased_p) // 2], increased_p[len(increased_p)//2], combined_p[len(combined_p) // 2]))
        if float(increased) > float(total) * 0.5:
            azs_increased += 1
        if combined_p[len(combined_p)//2] > 0:
            azs_median_increased += 1
        azs += 1
    print("azs increased/total azs: %d/%d=%f" % ( azs_increased, azs, float(azs_increased)/float(azs)))
    print("azs median increased/total azs: %d/%d=%f" % ( azs_median_increased, azs, float(azs_median_increased)/float(azs)))