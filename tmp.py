import re

s = "gcloud_metrics-day-1"
m = re.search(r"gcloud_metrics-day-([0-9]+)", s)
print(m[1])
