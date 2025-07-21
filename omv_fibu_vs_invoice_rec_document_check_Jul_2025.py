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
os.chdir("/Users/W513466/Documents/Data/OMV Full Pipeline Rec/Data/Jan (Repeat)/")


## Imports
df = pd.read_csv("output/Jan_repeat_doc_annotated_data_2025-07-15.csv", encoding='unicode_escape', dtype={"CARDNUMBER": "string", "MERCHANT_CONTRACTED_CLIENT": "string", "CLIENTCOUNTRYCODEINVOICE": "string"})

df[df['CUSTOMER_TYPE'] == 'TPCI']
df['IFCS_PRODUCTCODE'].dtype


## Cleaning
df[df['MERCHANT_CONTRACTED_CLIENT'].isna()]
df['TAX_CATEGORY'].unique()
df['CLIENTCOUNTRYCODEINVOICE'] = df['CLIENTCOUNTRYCODEINVOICE'].fillna("000")
df['MERCHANT_CONTRACTED_CLIENT'] = df['MERCHANT_CONTRACTED_CLIENT'].fillna("000")
df['CUSTVATRATE'] = df['INVOICE_CUSTVATRATE'].fillna("A1")


df['CUST_TRANSACTION_STATUS'] = df['CUST_TRANSACTION_STATUS'].fillna("None")
df['IA_TRANSACTION_STATUS'] = df['IA_TRANSACTION_STATUS'].fillna("None")
df['SITE_TRANSACTION_STATUS'] = df['SITE_TRANSACTION_STATUS'].fillna("None")
df['PRODUCTCODE_ROUTEX'] = df['PRODUCTCODE_ROUTEX'].fillna("000")
df['PRODUCTCODE_ROUTEX'] = df['PRODUCTCODE_ROUTEX'].astype(int)
df['PRODUCTCODE_ROUTEX'] = df['PRODUCTCODE_ROUTEX'].astype(str)

df[df['TAX_CATEGORY'].isna()]
df['TAX_CATEGORY'].unique()
df['TAX_CATEGORY_DESCRIPTION'].unique()
df[df['EXTERNALPRODUCTDESCRIPTION'] == 'Commission']

## Function Definitions
def eo_fibu(row):
    # Handle missing values for each individual row element
    if row['KD_FIBU_EXPECTED'] == 'Y':
        if row['CLIENTCOUNTRYCODEINVOICE'] == '040':
            if row['MERCHANT_CONTRACTED_CLIENT'] != '101':
                if row['TAX_CATEGORY'] != 'TAX CATEGORY 4':
                    if row['electricity'] != 'Y':
                        return 'Y'
    
    return 'N'
def icc_cust_fibu_1(row):
    #Handle missing values for each individual row element
    if row['KD_FIBU_EXPECTED'] == 'Y':
        if row['CLIENTCOUNTRYCODEINVOICE'] != '040':
            if row['CLIENTCOUNTRYCODEINVOICE'] != '000':
                return 'Y'

    return 'N'
def icc_cust_fibu_2(row):
    if row['KD_FIBU_EXPECTED'] == 'Y':
        if row['MERCHANT_CONTRACTED_CLIENT'] != '101':
            if row['elec_or_rc'] == 'Y':
                if row['CLIENTCOUNTRYCODEINVOICE'] == '040':
                    return 'Y'
    return 'N'
def icc_ia_sell_fibu(row):
    # Handle missing values for each individual row element
    if row['KD_FIBU_EXPECTED'] == 'Y':
        if row['CLIENTCOUNTRYCODEINVOICE'] == '040':
            if row['MERCHANT_CONTRACTED_CLIENT'] != '101':
                if row['TAX_CATEGORY'] != 'TAX CATEGORY 4':
                    if row['electricity'] != 'Y':
                        return 'Y'

    return 'N'
import pandas as pd

def check_statuses(row):
    # List of status values for easier manipulation
    statuses = [row['CUST_TRANSACTION_STATUS'], row['IA_TRANSACTION_STATUS'], row['SITE_TRANSACTION_STATUS']]
    
    # Count occurrences of "Deleted" in the statuses
    deleted_count = statuses.count("DELETED")
    
    # Return "mismatch" if there is 1 or 2 "Deleted" statuses and at least one other is not "Deleted"
    if 1 <= deleted_count <= 2 and deleted_count != len(statuses):
        return "mismatch"
    else:
        return "match"



## Fibus Expected
### Function Application
kd_fibu_marks = ['Standard', 'OWN']
ts_fibu_marks = ['OA']
fk_fibu_marks = ['TPCI']
pv_fibu_marks = ['Commission']
elec = ['980', '982', '984']
commis = ['9999999992']
df['electricity'] = np.where(df['PRODUCTCODE_ROUTEX'].isin(elec), 'Y', 'N')
df['KD_FIBU_EXPECTED'] = np.where((df['CUSTOMER_TYPE'].isin(kd_fibu_marks)) & (df['IFCS_PRODUCTCODE'] != 9999999992), 'Y', 'N')
df['TS_FIBU_EXPECTED'] = np.where(df['SUPPLIERPARTICIPANTROUTEXID'].isin(ts_fibu_marks), 'Y', 'N')
df['FK_FIBU_EXPECTED'] = np.where(df['CUSTOMER_TYPE'].isin(fk_fibu_marks), 'Y', 'N')
df['PV_FIBU_EXPECTED'] = np.where(df['EXTERNALPRODUCTDESCRIPTION'].isin(pv_fibu_marks), 'Y', 'N')

df['ST_FIBU_EXPECTED'] = np.where((df['CUSTOMER_TYPE'] == 'RTX') & (df['MERCHANT_CONTRACTED_CLIENT'] == '101'), 'Y', 'N')
df['elec_or_rc'] = np.where((df['electricity'] == 'Y') | (df['TAX_CATEGORY'] == 'TAX CATEGORY 4'), 'Y', 'N')


df['EO_FIBU_EXPECTED'] = df.apply(eo_fibu, axis=1)
df['icc_cust_fibu_exp_1'] = df.apply(icc_cust_fibu_1, axis=1)
df['icc_cust_fibu_exp_2'] = df.apply(icc_cust_fibu_2, axis=1)
df['ICC_CUST_FIBU_EXPECTED'] = np.where(((df['icc_cust_fibu_exp_1'] == 'Y') | (df['icc_cust_fibu_exp_2'] == 'Y')), 'Y', 'N')
df['icc_ia_sell_fibu'] = df.apply(icc_ia_sell_fibu, axis=1)
df['icc_ia_purch_fibu'] = np.where((df['CUSTOMER_TYPE'] == 'RTX') & (df['MERCHANT_CONTRACTED_CLIENT'] == '101'), 'Y', 'N')
df['ICC_IA_FIBU_EXPECTED'] = np.where(((df['icc_ia_sell_fibu'] == 'Y') | (df['icc_ia_purch_fibu'] == 'Y')), 'Y', 'N')


### Deleted trxn Overwrite
df['KD_FIBU_EXPECTED'] = np.where(df['CUST_TRANSACTION_STATUS'] == 'DELETED', 'N', df['KD_FIBU_EXPECTED'])
df['FK_FIBU_EXPECTED'] = np.where(df['CUST_TRANSACTION_STATUS'] == 'DELETED', 'N', df['FK_FIBU_EXPECTED'])
df['ICC_CUST_FIBU_EXPECTED'] = np.where(df['CUST_TRANSACTION_STATUS'] == 'DELETED', 'N', df['ICC_CUST_FIBU_EXPECTED'])
df['PV_FIBU_EXPECTED'] = np.where(df['CUST_TRANSACTION_STATUS'] == 'DELETED', 'N', df['PV_FIBU_EXPECTED'])
df['EO_FIBU_EXPECTED'] = np.where(df['IA_TRANSACTION_STATUS'] == 'DELETED', 'N', df['EO_FIBU_EXPECTED'])
df['ST_FIBU_EXPECTED'] = np.where(df['IA_TRANSACTION_STATUS'] == 'DELETED', 'N', df['ST_FIBU_EXPECTED'])
df['ICC_IA_FIBU_EXPECTED'] = np.where(df['IA_TRANSACTION_STATUS'] == 'DELETED', 'N', df['ICC_IA_FIBU_EXPECTED'])
df['TS_FIBU_EXPECTED'] = np.where(df['SITE_TRANSACTION_STATUS'] == 'DELETED', 'N', df['TS_FIBU_EXPECTED'])

### Status Mismatches
df['Status Match'] = df.apply(check_statuses, axis=1)
df[df['Status Match'] == 'mismatch']
df['Status Match'].unique()
df['CUST_TRANSACTION_STATUS'].unique()
cust_trxnn_deleted = df[df['CUST_TRANSACTION_STATUS'] == 'DELETED']
ia_trxn_deleted = df[df['IA_TRANSACTION_STATUS'] == 'DELETED']
site_trxn_deleted = df[df['SITE_TRANSACTION_STATUS'] == 'DELETED']
cust_trxnn_deleted['IA_TRANSACTION_STATUS'].unique()
cust_trxnn_deleted


## Actual
df['KD_FIBU_ACTUAL'] = np.where((~df['KD_FIBU'].isna()), 'Y', 'N')
df['FK_FIBU_ACTUAL'] = np.where((~df['FK_FIBU'].isna()), 'Y', 'N')
df['EO_FIBU_ACTUAL'] = np.where((~df['EO_FIBU'].isna()), 'Y', 'N')
df['ST_FIBU_ACTUAL'] = np.where((~df['ST_FIBU'].isna()), 'Y', 'N')
df['TS_FIBU_ACTUAL'] = np.where((~df['TS_FIBU'].isna()), 'Y', 'N')
df['PV_FIBU_ACTUAL'] = np.where((~df['PV_FIBU'].isna()), 'Y', 'N')
df['ICC_CUST_FIBU_ACTUAL'] = np.where((~df['ICC_Cust_FIBU'].isna()), 'Y', 'N')
df['ICC_IA_FIBU_ACTUAL'] = np.where((~df['ICC_IA_FIBU'].isna()), 'Y', 'N')

df['KD_EXP_V_ACT_MATCH'] = np.where((df['KD_FIBU_ACTUAL'] == df['KD_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['FK_EXP_V_ACT_MATCH'] = np.where((df['FK_FIBU_ACTUAL'] == df['FK_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['EO_EXP_V_ACT_MATCH'] = np.where((df['EO_FIBU_ACTUAL'] == df['EO_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['ST_EXP_V_ACT_MATCH'] = np.where((df['ST_FIBU_ACTUAL'] == df['ST_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['TS_EXP_V_ACT_MATCH'] = np.where((df['TS_FIBU_ACTUAL'] == df['TS_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['PV_EXP_V_ACT_MATCH'] = np.where((df['PV_FIBU_ACTUAL'] == df['PV_FIBU_EXPECTED']), 'Match', 'Non-Match')

df['ICC_CUST_EXP_V_ACT_MATCH'] = np.where((df['ICC_CUST_FIBU_ACTUAL'] == df['ICC_CUST_FIBU_EXPECTED']), 'Match', 'Non-Match')
df['ICC_IA_EXP_V_ACT_MATCH'] = np.where((df['ICC_IA_FIBU_ACTUAL'] == df['ICC_IA_FIBU_EXPECTED']), 'Match', 'Non-Match')

def check_for_non_match(row, columns_to_check):
    # Iterate over the specified columns and check if "Non-Match" exists
    for col in columns_to_check:
        if row[col] == "Non-Match":
            return "Y"
    return "N"

def check_for_fibus(row, columns_to_check):
    # Iterate over the specified columns and check if "Non-Match" exists
    for col in columns_to_check:
        if row[col].isna():
            return "N"
    return "Y"

columns_to_check = ['KD_EXP_V_ACT_MATCH', 'FK_EXP_V_ACT_MATCH', 'EO_EXP_V_ACT_MATCH', 'ST_EXP_V_ACT_MATCH', 'TS_EXP_V_ACT_MATCH', 'ICC_CUST_EXP_V_ACT_MATCH', 'ICC_IA_EXP_V_ACT_MATCH', 'PV_EXP_V_ACT_MATCH']

# Apply the function row by row, and assign the result to a new column
df['Discrepancies'] = df.apply(check_for_non_match, axis=1, columns_to_check=columns_to_check)
discrep_counts = df.groupby('Discrepancies')['ID_TRAN'].count().reset_index()


def status_match_discrep(row):
    if row['Status Match'] == 'mismatch':
        return 'Y'


    return row['Discrepancies']
df['Discrepancies'] = df.apply(status_match_discrep, axis=1)


## Table and Report Generation
discrep = df[df['Discrepancies'] == 'Y']

### Cleaning - Jan 2025
discrep = discrep.drop(columns={'CUSTVATRATE', 'electricity', 'elec_or_rc', 'icc_cust_fibu_exp_1', 'icc_cust_fibu_exp_2'})


### Client Investigation
client_discrep = discrep.groupby('SUPPLIERSHORTNAME')['ID_TRAN'].count().reset_index().sort_values(by=['ID_TRAN'], ascending=False)

discrep_tt = discrep[discrep['SUPPLIERSHORTNAME'] == 'TT']

discrep_Smatrics = discrep[discrep['SUPPLIERSHORTNAME'] == 'Smatrics']

discrep_bp = discrep[discrep['SUPPLIERSHORTNAME'] == 'BP']

discrep_eni = discrep[discrep['SUPPLIERSHORTNAME'] == 'Eni']


### Product Investigation
prod_discrep = discrep.groupby('EXTERNALPRODUCTDESCRIPTION')['ID_TRAN'].count().reset_index().sort_values(by=['ID_TRAN'], ascending=False)

toll_discrep = discrep[discrep['EXTERNALPRODUCTDESCRIPTION'] == 'OBU toll payment (no VAT)']

### Match / Non-Match Counts
matches = df[['KD_EXP_V_ACT_MATCH', 'FK_EXP_V_ACT_MATCH', 'EO_EXP_V_ACT_MATCH', 'ST_EXP_V_ACT_MATCH', 'TS_EXP_V_ACT_MATCH', 'ICC_CUST_EXP_V_ACT_MATCH', 'ICC_IA_EXP_V_ACT_MATCH', 'PV_EXP_V_ACT_MATCH']]


count_df = pd.DataFrame({
    'Match': matches.apply(lambda col: (col == 'Match').sum()),
    'Non-Match': matches.apply(lambda col: (col == 'Non-Match').sum())
})

df['FK_FIBU_EXPECTED'].unique()

df[~df['GROSSBALANCEIA'].isna()]


## Searches and Dives
df[df['POSTED_AT'] > '2024-11-01']

### ST Non Match
st_non_match = df[df['ST_EXP_V_ACT_MATCH'] == 'Non-Match']
st_non_match
st_non_match.groupby('CUST_TRANSACTION_STATUS')['ID_TRAN'].count()

### KD Non-Match
kd_non_match = df[df['KD_EXP_V_ACT_MATCH'] == 'Non-Match']
kd_non_match
kd_non_match.groupby('KD_FIBU_EXPECTED')['ID_TRAN'].count()
kd_non_match.groupby('CUSTOMER_TYPE')['ID_TRAN'].count()
kd_non_match.groupby('CUST_TRANSACTION_STATUS')['ID_TRAN'].count()
kd_non_match[kd_non_match['KD_FIBU_EXPECTED'] == 'N']

### EO
eo_discrep = discrep[discrep['EO_EXP_V_ACT_MATCH'] == 'Non-Match']
eo_discrep
eo_discrep.groupby('PRODUCTNAME_ROUTEX')['ID_TRAN'].count()

### ICC
icc_cust_discrep = discrep[discrep['ICC_CUST_EXP_V_ACT_MATCH'] == 'Non-Match']
icc_cust_discrep
icc_cust_discrep.groupby('CUST_TRANSACTION_STATUS')['ID_TRAN'].count()

icc_cust_discrep.groupby('PRODUCTNAME_ROUTEX')['ID_TRAN'].count()
df[df['ID_TRAN'] == 2758812]
icc_ia_discrep = discrep[discrep['ICC_IA_EXP_V_ACT_MATCH'] == 'Non-Match']
icc_ia_discrep
icc_ia_discrep.groupby('PRODUCTNAME_ROUTEX')['ID_TRAN'].count()
df = df.drop(columns={'electricity', 'elec_or_rc', 'icc_cust_fibu_exp_1', 'icc_cust_fibu_exp_2'})


## Exports

filepath = "output/2025-03-03/Feb/discrep.csv"
discrep.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/client_discrep.csv"
client_discrep.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/prod_discrep.csv"
prod_discrep.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/count_df.csv"
count_df.to_csv(filepath, index=True)

filepath = "output/2025-03-03/Feb/discrep_counts.csv"
discrep_counts.to_csv(filepath, index=False)

filepath = "output/2025-03-03/Feb/fibu_rec_full.csv"
df.to_csv(filepath, index=False)
