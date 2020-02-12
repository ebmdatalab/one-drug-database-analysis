# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Analysing the impact of the One Drug Database on OpenPrescribing
#
# February 2020
#
# The BSA publish the [Detailed Prescribing Information](https://www.nhsbsa.nhs.uk/prescription-data/prescribing-data/detailed-prescribing-information) dataset monthly, which contains the number of items prescribed of each presentation for each practice.  In 2020 they are changing the underlying database which produces this dataset.  This is called the [One Drug Database project](https://www.nhsbsa.nhs.uk/important-information-drug-data), and these changes will affect the Detailed Prescribing Information dataset that we use.
#
# We expect to be affected by two changes.
#
# ## Change 1: BNF code changes
#
# There will be a rationalisation of BNF codes.  In particular:
#
# > BNF codes will change for products (drugs and appliances) where the pack size is currently shown as part of the naming convention.
#
# [1501 presentations](#Distribution-of-numbers-of-new-presentations-that-are-mapped-to) are affected by changed BNF codes.
#
# However, it looks like there has been a much larger rationalisation of BNF codes.
#
# Changes include:
#
# * [Old "Liq Spec" presentation disaggregated into "oral solution" and "oral suspension" presentations](#Old-"Liq-Spec"-presentation-disaggregated-into-"oral-solution"-and-"oral-suspension"-presentations)
# * [Old brandend presentation disaggregated into branded and generic presentations](#Old-brandend-presentation-disaggregated-into-branded-and-generic-presentations)
# * [Old presentation disaggregated into more accurately named presentations](#Old-presentation-disaggregated-into-more-accurately-named-presentations)
# * [Branded appliances into branded presentation and generic presentation in chapter 13](#Branded-appliances-into-branded-presentation-and-generic-presentation-in-chapter-13)
# * [An old version of a presentation is combined with the current version of the same](#An-old-version-of-a-presentation-is-combined-with-the-current-version-of-the-same)
# * [An "Oral Soln" presentation is combined with an "Oral Susp" presentation](#An-"Oral-Soln"-presentation-is-combined-with-an-"Oral-Susp"-presentation)
# * [Presentations of different flavour are combined](#Presentations-of-different-flavour-are-combined)
# * [Presentations of different dose size are combined](#Presentations-of-different-dose-size-are-combined)
# * [Branded presentations are combined into a generic presentation](#Branded-presentations-are-combined-into-a-generic-presentation)
#
# Presentations affected by changed BNF codes account for [1.3% of items and 1.4% of net_cost in November 2019](#Percentage-of-items-and-net_cost-that-are-affected-by-change-1).
#
# 1% of presentations account for [55% of items](#Distribution-of-presentations-affected-by-change-1,-by-items) and [29% of net_cost](#Distribution-of-presentations-affected-by-change-1,-by-net_cost), and 10% of presentations account for [89% of items](#Distribution-of-presentations-affected-by-change-1,-by-items) and [82% of net_cost](#Distribution-of-presentations-affected-by-change-1,-by-net_cost).
#
# The 10 presentations most affected by changes in items are shown [here](#Top-10-presentations-affected-by-change-1,-by-items), and the 10 presentations most affected by changes in net_cost are shown [here](#Top-10-presentations-affected-by-change-1,-by-net_cost).
#
# ## Change 2: special container quantity changes
#
# Some special container packs currently include the pack size in the drug name, and the quantity field represents the number of packs.  Once the ODD changes are applied:
#
# > the quantity will be represented as the total quantity (for example, the number of mls or gms), instead of the number of special container packs.
#
# Presentations affected by changed BNF codes account for [1.5% of items and 3.2% of net_cost in November 2019](#Percentage-of-items-and-net_cost-that-are-affected-by-change-2).
#
# 1% of presentations account for [44% of items](#Distribution-of-presentations-affected-by-change-2,-by-items) and [31% of net_cost](#Distribution-of-presentations-affected-by-change-2,-by-net_cost), and 10% of presentations account for [87% of items](#Distribution-of-presentations-affected-by-change-2,-by-items) and [84% of net_cost](#Distribution-of-presentations-affected-by-change-2,-by-net_cost).
#
# The 10 presentations most affected by changes in items are shown [here](#Top-10-presentations-affected-by-change-2,-by-items), and the 10 presentations most affected by changes in net_cost are shown [here](#Top-10-presentations-affected-by-change-2,-by-net_cost).

# ---

# ## Housekeeping

# +
# Imports from the standard library
import json
from collections import Counter

# Imports from third-party packages
import pandas as pd
from ebmdatalab import bq
# -

# Ensure that DataFrames columns are not truncated
pd.set_option('display.max_colwidth', -1)

# ### Total items and net_cost for November 2019

# +
# Query the prescribing data in BigQuery for the total items and net_cost 
sql = """
SELECT
    SUM(items) AS items,
    SUM(net_cost_pence) / 100 AS net_cost
FROM public_draft.prescribing
WHERE month = '2019-11-01'
"""

df = bq.cached_read(sql, csv_path='../bq-cache/2019_11_items_and_spending.csv')
df
# -

# Save these off for later -- we'll want to see what proprtion the items and net_cost are affected by the changes
total_items, total_net_cost = df.iloc[0]

# ---

# ## Change 1: BNF code changes

# ### Affected presentations

# Load the data from the spreadsheet
bnf_mapping_raw = pd.read_excel('../data/MDR BNF to dm+d BNF Mapping.xlsx')
bnf_mapping_raw.head()

# +
# Massage the data a bit, changing column names...
bnf_mapping = bnf_mapping_raw.rename(columns={
    'Current BNF Code': 'old_code',
    'MDR: BNF Description': 'old_name',
    'dm+d: BNF Description': 'new_name',
    'New BNF Code': 'new_code',
    'BNF Code Changed (Y/N)': 'changed'
})
# ...dropping rows where the code hasn't changed...
bnf_mapping = bnf_mapping[bnf_mapping['changed'] == 'Y']
# ...dropping rows where the old_code is in chapter 19...
bnf_mapping = bnf_mapping[~bnf_mapping['old_code'].str.startswith('19')]
# ...dropping rows where the new_code is in chapter 19...
bnf_mapping = bnf_mapping[~bnf_mapping['new_code'].str.startswith('19')]
# ...and sorting by old_code and new_code
bnf_mapping = bnf_mapping.sort_values(['old_code', 'new_code'])

print(f"There are {len(bnf_mapping)} changed records")
display(bnf_mapping.head())
# -

# ### Distribution of numbers of new presentations that are mapped to
#
# _(Is there a better way to express this?)_

# There are 1501 distinct old BNF codes:

bnf_mapping['old_code'].nunique()

# There are 1418 old BNF codes map to a single new BNF code, 37 old BNF codes map to 2 new BNF codes... and 1 maps to 112 new BNF codes:

old_code_counts = Counter(bnf_mapping['old_code'])
old_code_count_distribution = sorted(Counter(old_code_counts.values()).items())
rows = [
    [num_old_codes, num_new_codes_mapped_to]
    for num_new_codes_mapped_to, num_old_codes in old_code_count_distribution
]
pd.DataFrame(rows, columns=['num_old_codes', 'num_new_codes_mapped_to'])

# ### Old presentations mapping to 2 new codes

# + scrolled=true
old_codes_mapping_to_2_new_codes = [code for code, count in old_code_counts.items() if count == 2]

with pd.option_context('display.max_rows', None):
    display(bnf_mapping[bnf_mapping["old_code"].isin(old_codes_mapping_to_2_new_codes)])
# -

# There are several patterns here that we can pick out:

# #### Old "Liq Spec" presentation disaggregated into "oral solution" and "oral suspension" presentations

bnf_mapping[bnf_mapping["old_code"] == "0102000L0AAAJAJ"]

# #### Old brandend presentation disaggregated into branded and generic presentations

bnf_mapping[bnf_mapping["old_code"] == "070405000BBADA0"]

# #### Old presentation disaggregated into more accurately named presentations

bnf_mapping[bnf_mapping["old_code"] == "0906027G0AAABAB"]

# #### Branded appliances into branded presentation and generic presentation in chapter 13

bnf_mapping[bnf_mapping["old_code"] == "21220000234"]

# ### Old presentation mapping to 112 new codes
#
# The old presentation covered a huge range of sizes, colours, and materials.  There is now one presentation for each.

bnf_code = old_code_counts.most_common(1)[0][0]
bnf_mapping[bnf_mapping["old_code"] == bnf_code].head(10)

# ### Distribution of numbers of old presentations that are mapped from
#
# _(Again, is there a better way to express this?)_

# There are 1476 distinct new BNF codes:

bnf_mapping['new_code'].nunique()

# There are 1270 new BNF codes that are mapped to from a single old BNF code, 132 new BNF codes that are mapped to from 2 old BNF codes... and 1 that is mapped to from 13 old BNF codes.

new_code_counts = Counter(bnf_mapping['new_code'])
new_code_count_distribution = sorted(Counter(new_code_counts.values()).items())
rows = [
    [num_new_codes, num_old_codes_mapped_from]
    for num_old_codes_mapped_from, num_new_codes in new_code_count_distribution
]
pd.DataFrame(rows, columns=['num_new_codes', 'num_old_codes_mapped_from'])

# ### New presentations mapped to from 2 new codes

# +
new_codes_mapping_to_2_new_codes = [code for code, count in new_code_counts.items() if count == 2]

with pd.option_context('display.max_rows', None):
    display(bnf_mapping[bnf_mapping["new_code"].isin(new_codes_mapping_to_2_new_codes)].sort_values(['new_code', 'old_code']))
# -

# Again, there are some patterns we can pick out:

# #### An old version of a presentation is combined with the current version of the same

bnf_mapping[bnf_mapping["new_code"] == "0102000ACAABABA"]

# #### An "Oral Soln" presentation is combined with an "Oral Susp" presentation

bnf_mapping[bnf_mapping["new_code"] == "0102000L0AAADAD"]

# #### Presentations of different flavour are combined

bnf_mapping[bnf_mapping["new_code"] == "0106010E0AAADAD"]

# #### Presentations of different dose size are combined

bnf_mapping[bnf_mapping["new_code"] == "0206010F0BFABCJ"]

# #### Branded presentations are combined into a generic presentation

bnf_mapping[bnf_mapping["new_code"] == "090504700AABCBC"]

# ### Total items and net_cost for each presentation affected by change 1 for November 2019

# +
joined_bnf_codes = ", ".join("'{}'".format(bnf_code) for bnf_code in bnf_mapping["old_code"])

sql = """
SELECT
    p.bnf_code,
    p.name,
    SUM(rx.items) AS items,
    SUM(rx.net_cost_pence) / 100 AS net_cost
FROM public_draft.prescribing rx
INNER JOIN public_draft.presentation p
    ON rx.bnf_code = p.bnf_code
WHERE rx.month = '2019-11-01'
  AND p.bnf_code IN ({})
GROUP BY p.bnf_code, p.name
""".format(joined_bnf_codes)

df1 = bq.cached_read(sql, csv_path='../bq-cache/2019_11_items_and_spending1.csv')
df1.set_index('bnf_code', inplace=True)
df1.head()
# -

items1, net_cost1 = df1['items'], df1['net_cost']

# ### Percentage of items and net_cost that are affected by change 1

items1.sum() / total_items * 100

net_cost1.sum() / total_net_cost * 100

# ### Distribution of presentations affected by change 1, by items

# Find the proprtion of presentations accounted for by the top 1%, 5%, 10%, 20% of prescribing, by items
for percentile in [0.01, 0.05, 0.1, 0.2]:
    print(percentile, items1[items1 > items1.quantile(1 - percentile)].sum() / items1.sum())

ax = items1.sort_values().cumsum().reset_index().plot()
ax.set_ylim(0);

# ### Top 10 presentations affected by change 1, by items

df1.sort_values(['items'], ascending=False).head(10)

# ### Distribution of presentations affected by change 1, by net_cost

# Find the proprtion of presentations accounted for by the top 1%, 5%, 10%, 20% of prescribing, by net_cost
for percentile in [0.01, 0.05, 0.1, 0.2]:
    print(percentile, net_cost1[net_cost1 > net_cost1.quantile(1 - percentile)].sum() / net_cost1.sum())

ax = net_cost1.sort_values().cumsum().reset_index().plot()
ax.set_ylim(0);

# ### Top 10 presentations affected by change 1, by net_cost

df1.sort_values(['net_cost'], ascending=False).head(10)

# ---

# ## Change 2: special container quantity changes

# ### Affected presentations

quantity_change = pd.read_excel('../data/Special Container size mismatch between MDR and dm+d latest.xlsx')
print(f"There are {len(quantity_change)} changed records")
display(quantity_change.head())

# ### Total items and net_cost for each presentation affected by change 2 for November 2019

# +
joined_bnf_codes = ", ".join("'{}'".format(bnf_code) for bnf_code in quantity_change['BNF Code'])

sql = """
SELECT
    p.bnf_code,
    p.name,
    SUM(rx.items) AS items,
    SUM(rx.net_cost_pence) / 100 AS net_cost
FROM public_draft.prescribing rx
INNER JOIN public_draft.presentation p
    ON rx.bnf_code = p.bnf_code
WHERE rx.month = '2019-11-01'
  AND p.bnf_code IN ({})
GROUP BY p.bnf_code, p.name
""".format(joined_bnf_codes)

df2 = bq.cached_read(sql, csv_path='../bq-cache/2019_11_items_and_spending2.csv')
df2.set_index('bnf_code', inplace=True)
df2.head()
# -

items2, net_cost2 = df2['items'], df2['net_cost']

# ### Percentage of items and net_cost that are affected by change 2

items2.sum() / total_items * 100

net_cost2.sum() / total_net_cost * 100

# ### Distribution of presentations affected by change 2, by items

# Find the proprtion of presentations accounted for by the top 1%, 5%, 10%, 20% of prescribing, by items
for percentile in [0.01, 0.05, 0.1, 0.2]:
    print(percentile, items2[items2 > items2.quantile(1 - percentile)].sum() / items2.sum())

ax = items2.sort_values().cumsum().reset_index().plot()
ax.set_ylim(0);

# ### Top 10 presentations affected by change 2, by items

df2.sort_values(['items'], ascending=False).head(10)

# ### Distribution of presentations affected by change 2, by net_cost

# Find the proprtion of presentations accounted for by the top 1%, 5%, 10%, 20% of prescribing, by net_cost
for percentile in [0.01, 0.05, 0.1, 0.2]:
    print(percentile, net_cost2[net_cost2 > net_cost2.quantile(1 - percentile)].sum() / net_cost2.sum())

ax = net_cost2.sort_values().cumsum().reset_index().plot()
ax.set_ylim(0);

# ### Top 10 presentations affected by change 2, by net_cost

df2.sort_values(['net_cost'], ascending=False).head(10)
