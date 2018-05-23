import itertools
import bisect

class PriceTimeSeriesNaive(object):
    def __init__(self, data):
        self.data = sorted(data, key=lambda x: x["timestamp"])

        prev = self.data[0]
        for record in self.data:
            prev["duration"] = record["timestamp"] - prev["timestamp"]
            prev = record
        self.data = self.data[:-1]
    
    def expand_with_interval(self, interval):
        for record in self.data:
            for x in range(0, round(record["duration"] / interval)):
                yield dict(record)

class PriceBucketNaive(object):
    def __init__(self, prices):
        self.by_ts = sorted(prices, key=lambda x: x["timestamp"])
        self.by_value = sorted(prices, key=lambda x: x["price"])

    def getQuantile(self, quant):
        return self.by_value[int(len(self.by_value) * quant)]

    def getAverage(self):
        return sum(record["price"] for record in self.by_ts) / float(len(self.by_ts))

# class PriceTimeSeries(object):
#     def __init__(self, data):
#         self.data = sorted(data, lambda x: x["timestamp"]

#     # reasonably confident in the correctness of this implementation
#     def split_at_interval(self, interval):
#         start = self.data[0]
#         next_split = start + interval

#         bucket = []
#         last_record = None 
#         for record in self.data:
#             last_record = record

#             if record["timestamp"] >= next_split:
#                 yield bucket 

#                 last_copy = dict(last_record)
#                 last_copy["timestamp"] = next_split
#                 bucket = [last_copy, record]

#                 next_split += interval 
#                 while record["timestamp"] >= next_split:
#                     yield bucket 
#                     record_copy = dict(record)
#                     record_copy["timestamp"] = next_split - interval
#                     bucket = [record_copy]
#             else:
#                 bucket.append(record)

# class PriceBucket(object):
#     def __init__(self, data):
#         self.data_by_ts = object 
#         self.data_by_price = sorted(data, lambda x: x["price"])
        
#         self.total_duration = self.data_by_ts[1]["timestamp"] - self.data_by_ts[0]["timestamp"]
