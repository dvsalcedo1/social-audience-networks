"""
Generates the shared sources network for a scan
"""

import pandas as pd
import itertools

filename = "Political study - NLP Facebook Data - 20211124-processed"
print(f"Processing data/{filename}.csv")
df = pd.read_csv(
    f"data/{filename}.csv",
    usecols=["linker_name","linker_type","linker_url","source_name","source_type","source_url"],
    dtype=str
)

# Determine number of agents per source
roots = pd.DataFrame()
for i in df.index.tolist():
    source = df.at[i,'source_url']
    dest = df.at[i,'linker_url']
    if source != dest:
        try:
            roots.at[dest,'connectors'] += 1
        except:
            roots.at[dest,'connectors'] = 1
roots = roots[ roots['connectors'] > 1 ].sort_values(by='connectors',ascending=False).reset_index()
roots = roots[(~roots['index'].isna()) & (roots['index'] != 'bit.ly')]

# Get shared sources
def shared_sources(pages,posts,site_ind1,site_ind2,threshold=0):
    site1 = pages.loc[site_ind1,'index']
    site2 = pages.loc[site_ind2,'index']
    sources1 = set(list(posts[ posts['linker_url'] == site1 ]['source_url']))
    sources2 = set(list(posts[ posts['linker_url'] == site2 ]['source_url']))
    common_sources = sources1.intersection(sources2)

    if len(common_sources) >= threshold:
        return {"shared": True, "commons": len(common_sources) }
    else:
        return {"shared": False, "commons": len(common_sources) }

import itertools
pages_df = roots
num_pages = len(roots)

links = []
commons = []
page_inds = pages_df.index.tolist()
pairs = list(itertools.combinations(page_inds,2))
counter = 0
for pair in pairs:
    total_pairs = num_pages*(num_pages-1)/2
    res = shared_sources(pages=pages_df,posts=df,site_ind1=pair[0],site_ind2=pair[1])
    link = res['shared']
    commons.append(res['commons'])
    if link == True:
        links.append({
            "site1": pages_df.loc[pair[0],'index'],
            "site2": pages_df.loc[pair[1],'index'],
            "link": res['commons']
        })
    counter+= 1
    if counter%1000 == 0:
        print(f"Processed {counter} of {total_pairs} pairs")
    if counter == total_pairs:
        print(f"Processed {counter} of {total_pairs} pairs")

links = pd.DataFrame(links)

links_final = links[links['link'] > 1].sort_values(by='link',ascending=False)
links_final.to_csv(f"results/{filename}-shared-sources-links.csv",index=False)
print(f"Exported file to results/{filename}-shared-sources-links.csv")

# Get the node names
linkers = df[["linker_name","linker_type","linker_url"]].drop_duplicates(subset=['linker_url'])
nodes = pd.merge(roots,linkers,left_on='index',right_on='linker_url',how='left')
nodes.to_csv(f"results/{filename}-shared-sources-nodes.csv",index=False)
print(f"Exported file to results/{filename}-shared-sources-nodes.csv")