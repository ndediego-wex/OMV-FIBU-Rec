## Set Up

Source all FIBU, invoice, and transaction files from the OMV database using the provided SQL queries, and save them in csv format.\
In the name of the directory, include the date it is run, and the month for which the reconciliation occurring.\
In a new directory, create two new folders named “inputs” and “outputs”.\
In the “inputs” folder make sure all files are named exactly the same as shown below.
****************************************************************************
CUSTOMER_INVOICED.csv\
EO_FIBU.csv\
FK_FIBU.csv\
IA_INVOICED.csv\
ICC_FIBU_CUST.csv\
ICC_FIBU_IA.csv\
KD_FIBU.csv\
PV_FIBU.csv\
SITE_INVOICED.csv\
ST_FIBU.csv\
TRANSACTIONS.csv\
TS_FIBU.csv
*****************************************************************************

## Running the Scripts

Open the script “omv_fibu_vs_invoice_rec” and change the filepath to point toward the directory containing the “input” and “output” folders.\
Run the script “omv_fibu_vs_invoice_rec”.\
Check the “output” folder and ensure the results are populating the folder.\
Open the script “omv_fibu_vs_invoice_rec_file_joins” and change the filepath to point toward the directory containing the “input” and “output” folders.\
Run the script “omv_fibu_vs_invoice_rec_file_joins”.\
Open the script “omv_fibu_vs_invoice_rec_document_check” and change the filepath to point toward the directory containing the “input” and “output” folders.\
Run the script “omv_fibu_vs_invoice_rec_document_check”.\
All results and outputs can be found in the “outputs” folder

## Script Function Overview
Omv_fibu_vs_invoice_rec\
This script compares the number of transactions found on FIBUs, invoices, and the transaction list.\
If there is a discrepancy in the number of transactions between these files, those transactions that cannot be accounted for will be listed in one of the results files in the “outputs” folder.\
Inv_no_fibu_full - details of transactions found on the invoice and not on FIBU\
Fibu_no_inv_full - details of transactions found on FIBU and not on the invoice\
Merge_counts - a table of counts that shows the number of transactions found only on FIBUs or invoices, and those found on both.\
Fibu_merge_counts - a table of counts that shows the number of transactions found only on FIBUs or the transaction extract, and those found on both.\
Inv_merge_counts - a table of counts that shows the number of transactions found only on invoices or the transaction extract, and those found on both.\
Inv_no_extract - details of transactions found on the invoice and not on the transaction extract.\
Fibu_no_extract - details of transactions found on FIBUs and not on the transaction extract.\
Extract_no_inv - details of transactions found on the transaction extract and not on the invoices.\
Extract_no_fibu - details of transactions found on the transaction extract and not on the FIBUs.


## Omv_fibu_vs_invoice_rec_file_joins
Joins the transaction extract to the fibus and invoices, to show which files each transactions are found on.\
The output of this script is:\
Annotated_data - a list of all transactions found on the transaction extract, and which FIBUs and invoices the transactions are found on.\
Input_filetype_counts - A table that displays the counts of transactions found on each individual fibu, and compares it to the number of transactions found on the annotated_data file output.



## Omv_fibu_vs_invoice_rec_document_check
Uses transaction details to determine which FIBU files transactions should be found on based on known criteria. Compares expected files with acutla files to determine whether there are any discrepancies or not.\
The output of this script is:\
Fibu_rec_full - a full list of all transactions, all files those transactions are expected to be found on, and all those on which they are actually found.\
Discrep - a list of all transactions that contain discrepancies between expected and actual listed filetypes\
Discrep_counts - A table breaking down the number of discrepancies\
Client_discrep - A table breaking down the number of discrepancies by client\
Prod_discrep - A table breaking down the number of discrepancies by product\
Count_df -  A table breaking down the number of discrepancies by file type
