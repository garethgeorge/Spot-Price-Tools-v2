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

    median_prices = []

    for result in data:
        az, insttype = tuple(result["key"])

        if "p2" not in insttype: continue

        delta = result["total_cost_after"] - result["total_cost_before"]

        median_prices.append(delta)

    median_prices.sort()

    print("median: ", median(median_prices))
    print("95th percentile: ", percentile(median_prices, 95))
    print("05th percentile: ", percentile(median_prices, 5))