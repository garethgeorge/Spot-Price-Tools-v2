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

    data.sort(key=lambda x: -(x["total_cost_after"] - x["total_cost_before"]))

    print("%30s %10s %10s %10s" % ("name", "before", "after", "delta"))
    for result in data:
        az, insttype = tuple(result["key"])

        delta = result["total_cost_after"] - result["total_cost_before"]

        print("%30s %10.2f %10.2f %10.2f" % (
            str(az) + "-" + str(insttype),
            result["total_cost_before"],
            result["total_cost_after"],
            delta
        ))
    