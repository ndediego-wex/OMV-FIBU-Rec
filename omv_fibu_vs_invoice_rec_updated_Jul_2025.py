import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.max_rows', 20)
# pd.set_option('display.precision', 2)
# pd.options.display.float_format = "{:,.2f}".format

os.chdir("/Users/W513466/Documents/Data/OMV Full Pipeline Rec/Data/Jan (Repeat)")


## Imports
fk_fibu = pd.read_csv("input/FK_FIBU.csv")
eo_fibu = pd.read_csv("input/EO_FIBU.csv")
icc_fibu_ia = pd.read_csv("input/ICC_FIBU_IA.csv")
icc_fibu_cust = pd.read_csv("input/ICC_FIBU_CUST.csv")
kd_fibu = pd.read_csv("input/KD_FIBU.csv")
st_fibu = pd.read_csv("input/ST_FIBU.csv")
ts_fibu = pd.read_csv("input/TS_FIBU.csv")
pv_fibu = pd.read_csv("input/PV_FIBU.csv")

cust_inv = pd.read_csv("input/CUSTOMER_INVOICED.csv")
ia_inv = pd.read_csv("input/IA_INVOICED.csv")
site_inv = pd.read_csv("input/SITE_INVOICED.csv")

trxn_ext = pd.read_csv("input/TRANSACTIONS.csv", encoding='unicode_escape', dtype={"CARDNUMBER": "string", "MERCHANT_CONTRACTED_CLIENT": "string", "CLIENTCOUNTRYCODEINVOICE": "string"})



### Cleaning
fk_fibu = fk_fibu.rename(columns={'FK_FIBU': "FIBU"})
eo_fibu = eo_fibu.rename(columns={'EO_FIBU': "FIBU"})
icc_fibu_cust = icc_fibu_cust.rename(columns={'ICC_FIBU': "FIBU"})
icc_fibu_ia = icc_fibu_ia.rename(columns={'ICC_FIBU': "FIBU"})
kd_fibu = kd_fibu.rename(columns={'KD_FIBU': "FIBU"})
st_fibu = st_fibu.rename(columns={'ST_FIBU': "FIBU"})
ts_fibu = ts_fibu.rename(columns={'TS_FIBU': "FIBU"})
pv_fibu = pv_fibu.rename(columns={'KD_FIBU': "FIBU"})


cust_inv = cust_inv.rename(columns={'CUSTOMER_DOCUMENTNUMBER': "DOCUMENTNUMBER", "CUSTOMER_DOCUMENT_TYPE": "DOCUMENT_TYPE", "CUSTOMER_INVOICEDATE": "INVOICEDATE"})
ia_inv = ia_inv.rename(columns={'IA_DOCUMENTNUMBER': "DOCUMENTNUMBER", "IA_DOCUMENT_TYPE": "DOCUMENT_TYPE", "IA_INVOICEDATE": "INVOICEDATE"})
site_inv = site_inv.rename(columns={'SITE_DOCUMENTNUMBER': "DOCUMENTNUMBER", "SITE_DOCUMENT_TYPE": "DOCUMENT_TYPE", "SITE_INVOICEDATE": "INVOICEDATE"})

cust_inv['inv_type'] = 'CUST'
ia_inv['inv_type'] = 'IA'
site_inv['inv_type'] = 'SITE'
fk_fibu['fibu_type'] = 'FK'
eo_fibu['fibu_type'] = 'EO'
icc_fibu_ia['fibu_type'] = 'ICC'
icc_fibu_cust['fibu_type'] = 'ICC_Cust'
kd_fibu['fibu_type'] = 'KD'
st_fibu['fibu_type'] = 'ST'
ts_fibu['fibu_type'] = 'TS'
pv_fibu['fibu_type'] = 'PV'


trxn_ext = trxn_ext.drop_duplicates(subset=['ID_TRAN'])
trxn_ext["POSTED_AT"] = pd.to_datetime(trxn_ext["POSTED_AT"], format='%d-%b-%y %H.%M.%S.%f %p')
trxn_ext["TRANSACTIONDATE"] = pd.to_datetime(trxn_ext["TRANSACTIONDATE"], format='%d-%b-%y %H.%M.%S.%f %p')
trxn_ext['POSTED_AT_date_only'] = trxn_ext['POSTED_AT'].dt.normalize()


## Joining
fibu = pd.concat([fk_fibu, eo_fibu, icc_fibu_cust, icc_fibu_ia, kd_fibu, st_fibu, ts_fibu, pv_fibu])

invoice = pd.concat([cust_inv, ia_inv, site_inv])

# invoice = pd.concat([cust_inv, ia_inv, routex_inv, site_inv])
fibu_invoice_merge = pd.merge(fibu, invoice, on='ID_TRAN', how='outer', suffixes=('_FIBU', '_INV'), indicator=True)
merge_counts = fibu_invoice_merge.groupby('_merge')['ID_TRAN'].count().reset_index()


fibu_extract_merge = pd.merge(fibu, trxn_ext, on='ID_TRAN', how='outer', suffixes=('_FIBU', '_EXT'), indicator=True)
fibu_merge_counts = fibu_extract_merge.groupby('_merge')['ID_TRAN'].count().reset_index()


fibu_extract_merge['ID_TRAN'].is_unique
fibu_extract_merge = fibu_extract_merge.drop_duplicates(subset=['ID_TRAN'])
fibu_merge_counts = fibu_extract_merge.groupby('_merge')['ID_TRAN'].count().reset_index()


inv_extract_merge = pd.merge(invoice, trxn_ext, on='ID_TRAN', how='outer', suffixes=('_INV', '_EXT'), indicator=True)
inv_merge_counts = inv_extract_merge.groupby('_merge')['ID_TRAN'].count().reset_index()


fibu_only = fibu_invoice_merge[fibu_invoice_merge['_merge'] == 'left_only']

inv_only = fibu_invoice_merge[fibu_invoice_merge['_merge'] == 'right_only']
inv_no_extract = inv_extract_merge[inv_extract_merge['_merge'] == 'left_only']
fibu_no_extract = fibu_extract_merge[fibu_extract_merge['_merge'] == 'left_only']
extract_no_inv = inv_extract_merge[inv_extract_merge['_merge'] == 'right_only']
extract_no_fibu = fibu_extract_merge[fibu_extract_merge['_merge'] == 'right_only']

inv_no_extract = inv_no_extract.dropna(axis=1, how='all')
fibu_no_extract = fibu_no_extract.dropna(axis=1, how='all')
fibu_only.groupby('fibu_type')['ID_TRAN'].count().reset_index()
inv_only.groupby('inv_type')['ID_TRAN'].count().reset_index()
inv_only.groupby('DOCUMENTNUMBER')['ID_TRAN'].count().reset_index().sort_values("ID_TRAN", ascending=False)

fibu_only_full = pd.merge(fibu_only, trxn_ext, on='ID_TRAN', how='left')
inv_only_full = pd.merge(inv_only, trxn_ext, on='ID_TRAN', how='left')

posted_dates = inv_only_full.groupby('POSTED_AT_date_only')['ID_TRAN'].count().reset_index()
posted_dates = posted_dates.rename(columns={'ID_TRAN': 'Count'})


posted_dates = fibu_only_full.groupby('POSTED_AT_date_only')['ID_TRAN'].count().reset_index()
posted_dates = posted_dates.rename(columns={'ID_TRAN': 'Count'})

inv_only_full.groupby('CUST_TRANSACTION_STATUS')['ID_TRAN'].count()
inv_only_full["INVOICEDATE"] = pd.to_datetime(inv_only_full["INVOICEDATE"], format='%d-%b-%y %H.%M.%S.%f %p')
inv_only_full['INVOICEDATE_date_only'] = inv_only_full['INVOICEDATE'].dt.normalize()


inv_dates = inv_only_full.groupby('INVOICEDATE_date_only')['ID_TRAN'].count().reset_index()
inv_dates = inv_dates.rename(columns={'ID_TRAN': 'Count'})

### Fibu Counts
input_fibu_file_counts = fibu.groupby("fibu_type")['ID_TRAN'].count().reset_index()
input_fibu_file_counts

## Export

filepath = "output/2025-03-03/Feb/inv_no_fibu_full.csv"
inv_only_full.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/fibu_no_inv_full.csv"
fibu_only_full.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/inv_dates.csv"
inv_dates.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/posted_dates.csv"
posted_dates.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/merge_counts.csv"
merge_counts.to_csv(filepath, index=False)


filepath = "output/2025-03-03/Feb/inv_no_extract.csv"
inv_no_extract.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/fibu_no_extract.csv"
fibu_no_extract.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/extract_no_inv.csv"
extract_no_inv.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/extract_no_fibu.csv"
extract_no_fibu.to_csv(filepath, index=False)
