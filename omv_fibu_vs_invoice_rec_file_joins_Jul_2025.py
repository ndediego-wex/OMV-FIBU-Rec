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


col_names = list(trxn_ext.columns)
trxn_ext = trxn_ext.drop_duplicates(subset=['ID_TRAN'])


## Cleaning
fk_fibu = fk_fibu.rename(columns={'FK_FIBU': "FIBU"})
eo_fibu = eo_fibu.rename(columns={'EO_FIBU': "FIBU"})
icc_fibu_cust = icc_fibu_cust.rename(columns={'ICC_FIBU': "FIBU"})
icc_fibu_ia = icc_fibu_ia.rename(columns={'ICC_FIBU': "FIBU"})
kd_fibu = kd_fibu.rename(columns={'KD_FIBU': "FIBU"})
st_fibu = st_fibu.rename(columns={'ST_FIBU': "FIBU"})
ts_fibu = ts_fibu.rename(columns={'TS_FIBU': "FIBU"})
pv_fibu = pv_fibu.rename(columns={'KD_FIBU': "FIBU"})


fk_fibu = fk_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
eo_fibu = eo_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
icc_fibu_cust = icc_fibu_cust.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
icc_fibu_ia = icc_fibu_ia.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
kd_fibu = kd_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
st_fibu = st_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
ts_fibu = ts_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])
pv_fibu = pv_fibu.drop(columns = ['ID_TXCT', 'ICP_TRANSACTION_POSTING_DATE'])

cust_inv = cust_inv.rename(columns={'CUSTOMER_DOCUMENTNUMBER': "DOCUMENTNUMBER", "CUSTOMER_DOCUMENT_TYPE": "DOCUMENT_TYPE", "CUSTOMER_INVOICEDATE": "INVOICEDATE"})
ia_inv = ia_inv.rename(columns={'IA_DOCUMENTNUMBER': "DOCUMENTNUMBER", "IA_DOCUMENT_TYPE": "DOCUMENT_TYPE", "IA_INVOICEDATE": "INVOICEDATE"})
site_inv = site_inv.rename(columns={'SITE_DOCUMENTNUMBER': "DOCUMENTNUMBER", "SITE_DOCUMENT_TYPE": "DOCUMENT_TYPE", "SITE_INVOICEDATE": "INVOICEDATE"})


cust_inv = cust_inv.drop(columns = ['ID_INIS', 'ICP_TRANSACTION_POSTING_DATE', 'DOCUMENT_TYPE'])
ia_inv = ia_inv.drop(columns = ['ID_INIS', 'ICP_TRANSACTION_POSTING_DATE', 'DOCUMENT_TYPE'])
site_inv = site_inv.drop(columns = ['ID_INIS', 'ICP_TRANSACTION_POSTING_DATE', 'DOCUMENT_TYPE'])

cust_inv['inv_type'] = 'CUST'
ia_inv['inv_type'] = 'IA'
site_inv['inv_type'] = 'SITE'

fk_fibu['fibu_type'] = 'FK'
eo_fibu['fibu_type'] = 'EO'
icc_fibu_ia['fibu_type'] = 'ICC_IA'
icc_fibu_cust['fibu_type'] = 'ICC_Cust'
kd_fibu['fibu_type'] = 'KD'
st_fibu['fibu_type'] = 'ST'
ts_fibu['fibu_type'] = 'TS'
pv_fibu['fibu_type'] = 'PV'

fibu_all = pd.concat([fk_fibu, eo_fibu, icc_fibu_cust, icc_fibu_ia, kd_fibu, st_fibu, ts_fibu, pv_fibu])

fk_fibu = fk_fibu.drop(columns = ['fibu_type'])
eo_fibu = eo_fibu.drop(columns = ['fibu_type'])
icc_fibu_cust = icc_fibu_cust.drop(columns = ['fibu_type'])
icc_fibu_ia = icc_fibu_ia.drop(columns = ['fibu_type'])
kd_fibu = kd_fibu.drop(columns = ['fibu_type'])
st_fibu = st_fibu.drop(columns = ['fibu_type'])
ts_fibu = ts_fibu.drop(columns = ['fibu_type'])
pv_fibu = pv_fibu.drop(columns = ['fibu_type'])

trxn_ext["POSTED_AT"] = pd.to_datetime(trxn_ext["POSTED_AT"], format='%d-%b-%y %H.%M.%S.%f %p')
trxn_ext["TRANSACTIONDATE"] = pd.to_datetime(trxn_ext["TRANSACTIONDATE"], format='%d-%b-%y %H.%M.%S.%f %p')

cust_inv["INVOICEDATE"] = pd.to_datetime(cust_inv["INVOICEDATE"], format='%d-%b-%y %H.%M.%S.%f %p')
ia_inv["INVOICEDATE"] = pd.to_datetime(ia_inv["INVOICEDATE"], format='%d-%b-%y %H.%M.%S.%f %p')
site_inv["INVOICEDATE"] = pd.to_datetime(site_inv["INVOICEDATE"], format='%d-%b-%y %H.%M.%S.%f %p')


## Joins
trxn_ext_2 = pd.merge(trxn_ext, pv_fibu, on='ID_TRAN', how='left')
trxn_ext_2 = trxn_ext_2.rename(columns={'FIBU': 'PV_FIBU'})
trxn_ext_2[~trxn_ext_2['PV_FIBU'].isna()]

trxn_ext_3 = pd.merge(trxn_ext_2, icc_fibu_cust, on='ID_TRAN', how='left')
trxn_ext_3 = trxn_ext_3.rename(columns={'FIBU': 'ICC_Cust_FIBU'})
trxn_ext_3[~trxn_ext_3['ICC_Cust_FIBU'].isna()]

trxn_ext_4 = pd.merge(trxn_ext_3, fk_fibu, on='ID_TRAN', how='left')
trxn_ext_4 = trxn_ext_4.rename(columns={'FIBU': 'FK_FIBU'})
trxn_ext_4[~trxn_ext_4['FK_FIBU'].isna()]

trxn_ext_5 = pd.merge(trxn_ext_4, eo_fibu, on='ID_TRAN', how='left')
trxn_ext_5 = trxn_ext_5.rename(columns={'FIBU': 'EO_FIBU'})

trxn_ext_6 = pd.merge(trxn_ext_5, icc_fibu_ia, on='ID_TRAN', how='left')
trxn_ext_6 = trxn_ext_6.rename(columns={'FIBU': 'ICC_IA_FIBU'})

trxn_ext_7 = pd.merge(trxn_ext_6, kd_fibu, on='ID_TRAN', how='left')
trxn_ext_7 = trxn_ext_7.rename(columns={'FIBU': 'KD_FIBU'})
trxn_ext_7[~trxn_ext_7['KD_FIBU'].isna()]

trxn_ext_8 = pd.merge(trxn_ext_7, st_fibu, on='ID_TRAN', how='left')
trxn_ext_8 = trxn_ext_8.rename(columns={'FIBU': 'ST_FIBU'})
trxn_ext_8[~trxn_ext_8['ST_FIBU'].isna()]

trxn_ext_9 = pd.merge(trxn_ext_8, ts_fibu, on='ID_TRAN', how='left')
trxn_ext_9 = trxn_ext_9.rename(columns={'FIBU': 'TS_FIBU'})
trxn_ext_9[~trxn_ext_9['TS_FIBU'].isna()]

trxn_ext_10 = pd.merge(trxn_ext_9, cust_inv, on='ID_TRAN', how='left')
trxn_ext_10 = trxn_ext_10.rename(columns={'INVOICEDATE': 'CUSTOMER_INVOICEDATE', 'DOCUMENTNUMBER': 'CUSTOMER_INVOICE_DOCUMENTNUMBER'})
trxn_ext_10[~trxn_ext_10['CUSTOMER_INVOICE_DOCUMENTNUMBER'].isna()]


trxn_ext_11 = pd.merge(trxn_ext_10, ia_inv, on='ID_TRAN', how='left')
trxn_ext_11 = trxn_ext_11.rename(columns={'DOCUMENTNUMBER': 'IA_INVOICE_DOCUMENTNUMBER', 'INVOICEDATE': 'IA_INVOICEDATE'})
trxn_ext_11[~trxn_ext_11['IA_INVOICE_DOCUMENTNUMBER'].isna()]


trxn_ext_fin = pd.merge(trxn_ext_11, site_inv, on='ID_TRAN', how='left')
trxn_ext_fin = trxn_ext_fin.rename(columns={'DOCUMENTNUMBER': 'SITE_INVOICE_DOCUMENTNUMBER', 'INVOICEDATE': 'SITE_INVOICEDATE'})

trxn_ext_fin[~trxn_ext_fin['SITE_INVOICE_DOCUMENTNUMBER'].isna()]

trxn_ext_fin = trxn_ext_fin [['ID_TRAN',
 'EXTERNALTRANSACTIONID',
 'POSTED_AT',
 'TRANSACTIONDATE',
 'CUST_TRANSACTION_STATUS',
 'IA_TRANSACTION_STATUS',
 'SITE_TRANSACTION_STATUS',
 'CLIENTCOUNTRYCODEINVOICE',
 'ISO4217ALPHA',
 'TAX_CATEGORY',
 'TAX_CATEGORY_DESCRIPTION',
 'BATCHSOURCE',
 'MERCHANT_CONTRACTED_CLIENT',
 'EXTERNALSITEID',
 'SUPPLIERSHORTNAME',
 'SUPPLIERPARTICIPANTROUTEXID',
 'CUSTOMER_TYPE',
 'CARDNUMBER',
 'CARD_ISSUER',
 'CUST_INVOICE_ISSUER',
 'TRXFREETEXT',
 'IFCS_PRODUCTCODE',
 'PRODUCTCODE_ROUTEX',
 'PRODUCTNAME_ROUTEX',
 'EXTERNALPRODUCTDESCRIPTION',
 'RECORDTYPE',
 'PRICETYPE',
 'BESTOFPRICING',
 'ALTERNATIVE_PRICE_TYPE',
 'QUANTITY',
 'POSVATRATE',
 'INVOICE_VOUCHERLINEITEMAMOUNT',
 'SUPPLY_VOUCHERLINEITEMAMOUNT',
 'ISSUER_VOUCHERLINEITEMAMOUNT',
 'INVOICE_POSNETAMOUNT',
 'SUPPLY_POSNETAMOUNT',
 'ISSUER_POSNETAMOUNT',
 'INVOICE_CUSTVATRATE',
 'INVOICE_NETCUSTBALANCE',
 'INVOICE_CUSTVATBALANCEAMOUNT',
 'INVOICE_TOTALGROSSCUSTBALANCE',
 'INVOICE_NETCUSTDISCOUNTAMOUNT',
 'INVOICE_NETCUSTSURCHARGEAMOUNT',
 'SUPPLY_CUSTVATRATE',
 'SUPPLY_NETCUSTBALANCE',
 'SUPPLY_CUSTVATBALANCEAMOUNT',
 'SUPPLY_TOTALGROSSCUSTBALANCE',
 'SUPPLY_NETCUSTDISCOUNTAMOUNT',
 'SUPPLY_NETCUSTSURCHARGEAMOUNT',
 'ISSUER_CUSTVATRATE',
 'ISSUER_NETCUSTBALANCE',
 'ISSUER_CUSTVATBALANCEAMOUNT',
 'ISSUER_TOTALGROSSCUSTBALANCE',
 'ISSUER_NETCUSTDISCOUNTAMOUNT',
 'ISSUER_NETCUSTSURCHARGEAMOUNT',
 'VATCOSTRATE',
 'NETCOSTBALANCE',
 'VATCOSTBALANCE',
 'GROSSCOSTBALANCE',
 'NETCOSTDISCOUNTAMOUNT',
 'NETCOSTSURCHARGEAMOUNT',
 'VATRATEIA',
 'NETBALANCEIA',
 'VATAMOUNTIA',
 'GROSSBALANCEIA',
 'NETDISCOUNTAMOUNTIA',
 'NETSURCHARGEAMOUNTIA',
 "CUSTOMER_INVOICE_DOCUMENTNUMBER",
 "CUSTOMER_INVOICEDATE",
 "SITE_INVOICE_DOCUMENTNUMBER",
 "SITE_INVOICEDATE",
 "IA_INVOICE_DOCUMENTNUMBER",
 "IA_INVOICEDATE",
 "KD_FIBU",
 "FK_FIBU",
 "ICC_Cust_FIBU",
 "TS_FIBU",
 "ST_FIBU",
 "EO_FIBU",
 "PV_FIBU",
 "ICC_IA_FIBU"]]



### Input vs output count compairison
######2025-03-19
input_filetype_conunts = fibu_all.groupby('fibu_type')['ID_TRAN'].count().reset_index()
input_filetype_conunts = input_filetype_conunts.rename(columns={'ID_TRAN': "input_counts"})
input_filetype_conunts
input_filetype_conunts['output_counts'] = [len(trxn_ext_fin[~trxn_ext_fin['EO_FIBU'].isna()]), len(trxn_ext_fin[~trxn_ext_fin['FK_FIBU'].isna()]), len(trxn_ext_fin[~trxn_ext_fin['ICC_Cust_FIBU'].isna()]),
                                           len(trxn_ext_fin[~trxn_ext_fin['ICC_IA_FIBU'].isna()]), len(trxn_ext_fin[~trxn_ext_fin['KD_FIBU'].isna()]), len(trxn_ext_fin[~trxn_ext_fin['PV_FIBU'].isna()]),
                                           len(trxn_ext_fin[~trxn_ext_fin['ST_FIBU'].isna()]), len(trxn_ext_fin[~trxn_ext_fin['TS_FIBU'].isna()])]

input_filetype_conunts
input_filetype_conunts['count_match'] = np.where(input_filetype_conunts['input_counts'] == input_filetype_conunts['output_counts'], 'match', 'non-match')
input_filetype_conunts
input_filetype_conunts['count_diff'] = abs(input_filetype_conunts['input_counts'] - input_filetype_conunts['output_counts'])
input_filetype_conunts


## Exports

filepath = "output/2025-03-03/feb_doc_annotated_data_2025-03-03.csv"
trxn_ext_fin.to_csv(filepath, index=False)

filepath = "output/2025-03-03/input_filetype_conunts.csv"
input_filetype_conunts.to_csv(filepath, index=False)