import os
data_path = 'alldata.tsv'
output_dir = 'slides'


with open(data_path, 'r') as f:
    for line in f:
        _, _, _, _, url, _, _ = line.strip().split('\t')
        print(url)
        