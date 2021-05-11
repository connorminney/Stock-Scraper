
# Import Dependencies #
import time
from sympy import init_printing
init_printing()
from urllib.request import urlopen
from yahoo_fin import stock_info as sp
from datetime import date, datetime, timedelta
import math
import os
from pathlib import Path
import statistics
import glob
import random
import numpy as np
import pandas as pd
from threading import Thread
from lxml import html
import requests
from math import sqrt

# Set base interest rate 
Risk_Free_Rate = .0025

# Find local downloads path #
download_path = str(os.path.join(Path.home(), "Downloads"))

# Create today variable and a today - 1 year variable
today = datetime.today().strftime('%Y-%m-%d')
last_year = datetime.today() - timedelta(days=365)
last_year = last_year.strftime('%Y-%m-%d')

# Format pandas to remove scientific notation
pd.options.display.float_format = '{:.2f}'.format

''' Create a list of tickers to scan '''

# Russell 1000 #
Tickers = ['TWOU', 'MMM', 'ABT', 'ABBV', 'ABMD', 'ACHC', 'ACN', 'ATVI', 'AYI', 'ADNT', 'ADBE', 'ADT', 'AAP', 'AMD', 'ACM', 'AES', 'AMG', 'AFL', 'AGCO', 'A', 'AGIO', 'AGNC', 'AL', 'APD', 'AKAM', 'ALK', 'ALB', 'AA', 'ARE', 'ALXN', 'ALGN', 'ALKS', 'Y', 'ALLE', 'AGN', 'ADS', 'LNT', 'ALSN', 'ALL', 'ALLY', 'ALNY', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCX', 'DOX', 'UHAL', 'AEE', 'AAL', 'ACC', 'AEP', 'AXP', 'AFG', 'AMH', 'AIG', 'ANAT', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'NLY', 'ANSS', 'AR', 'ANTM', 'AON', 'APA', 'AIV', 'APY', 'APLE', 'AAPL', 'AMAT', 'ATR', 'APTV', 'WTR', 'ARMK', 'ACGL', 'ADM', 'ARNC', 'ARD', 'ANET', 'AWI', 'ARW', 'ASH', 'AZPN', 'ASB', 'AIZ', 'AGO', 'T', 'ATH', 'TEAM', 'ATO', 'ADSK', 'ADP', 'AN', 'AZO', 'AVB', 'AGR', 'AVY', 'AVT', 'EQH', 'AXTA', 'AXS', 'BKR', 'BLL', 'BAC', 'BOH', 'BK', 'OZK', 'BKU', 'BAX', 'BDX', 'WRB', 'BRK.B', 'BERY', 'BBY', 'BYND', 'BGCP', 'BIIB', 'BMRN', 'BIO', 'TECH', 'BKI', 'BLK', 'HRB', 'BLUE', 'BA', 'BOKF', 'BKNG', 'BAH', 'BWA', 'BSX', 'BDN', 'BFAM', 'BHF', 'BMY', 'BRX', 'AVGO', 'BR', 'BPYU', 'BRO', 'BFA', 'BFB', 'BRKR', 'BC', 'BG', 'BURL', 'BWXT', 'CHRW', 'CABO', 'CBT', 'COG', 'CACI', 'CDNS', 'CZR', 'CPT', 'CPB', 'CMD', 'COF', 'CAH', 'CSL', 'KMX', 'CCL', 'CRI', 'CASY', 'CTLT', 'CAT', 'CBOE', 'CBRE', 'CBS', 'CDK', 'CDW', 'CE', 'CELG', 'CNC', 'CDEV', 'CNP', 'CTL', 'CDAY', 'BXP', 'CF', 'CRL', 'CHTR', 'CHE', 'LNG', 'CHK', 'CVX', 'CIM', 'CMG', 'CHH', 'CB', 'CHD', 'CI', 'XEC', 'CINF', 'CNK', 'CTAS', 'CSCO', 'CIT', 'C', 'CFG', 'CTXS', 'CLH', 'CLX', 'CME', 'CMS', 'CNA', 'CNX', 'KO', 'CGNX', 'CTSH', 'COHR', 'CFX', 'CL', 'CLNY', 'CXP', 'COLM', 'CMCSA', 'CMA', 'CBSH', 'COMM', 'CAG', 'CXO', 'CNDT', 'COP', 'ED', 'STZ', 'CERN', 'CPA', 'CPRT', 'CLGX', 'COR', 'GLW', 'OFC', 'CSGP', 'COST', 'COTY', 'CR', 'CACC', 'CCI', 'CCK', 'CSX', 'CUBE', 'CFR', 'CMI', 'CW', 'CVS', 'CY', 'CONE', 'DHI', 'DHR', 'DRI', 'DVA', 'SITC', 'DE', 'DELL', 'DAL', 'XRAY', 'DVN', 'DXCM', 'FANG', 'DKS', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DIS', 'DHC', 'DOCU', 'DLB', 'DG', 'DLTR', 'D', 'DPZ', 'CLR', 'COO', 'DEI', 'DOV', 'DD', 'DPS', 'DTE', 'DUK', 'DRE', 'DNB', 'DNKN', 'DXC', 'ETFC', 'EXP', 'EWBC', 'EMN', 'ETN', 'EV', 'EBAY', 'SATS', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ESRT', 'EHC', 'EGN', 'ENR', 'ETR', 'EVHC', 'EOG', 'EPAM', 'EPR', 'EQT', 'EFX', 'EQIX', 'EQC', 'ELS', 'EQR', 'ERIE', 'ESS', 'EL', 'EEFT', 'EVBG', 'EVR', 'RE', 'EVRG', 'ES', 'UFS', 'DCI', 'EXPE', 'EXPD', 'STAY', 'EXR', 'XOG', 'XOM', 'FFIV', 'FB', 'FDS', 'FICO', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FEYE', 'FAF', 'FCNCA', 'FDC', 'FHB', 'FHN', 'FRC', 'FSLR', 'FE', 'FISV', 'FLT', 'FLIR', 'FND', 'FLO', 'FLS', 'FLR', 'FMC', 'FNB', 'FNF', 'FL', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'BEN', 'FCX', 'AJG', 'GLPI', 'GPS', 'EXAS', 'EXEL', 'EXC', 'GTES', 'GLIBA', 'GD', 'GE', 'GIS', 'GM', 'GWR', 'G', 'GNTX', 'GPC', 'GILD', 'GPN', 'GL', 'GDDY', 'GS', 'GT', 'GRA', 'GGG', 'EAF', 'GHC', 'GWW', 'LOPE', 'GPK', 'GRUB', 'GWRE', 'HAIN', 'HAL', 'HBI', 'THG', 'HOG', 'HIG', 'HAS', 'HE', 'HCA', 'HDS', 'HTA', 'PEAK', 'HEI.A', 'HEI', 'HP', 'JKHY', 'HLF', 'HSY', 'HES', 'GDI', 'GRMN', 'IT', 'HGV', 'HLT', 'HFC', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HHC', 'HPQ', 'HUBB', 'HPP', 'HUM', 'HBAN', 'HII', 'HUN', 'H', 'IAC', 'ICUI', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INGR', 'PODD', 'IART', 'INTC', 'IBKR', 'ICE', 'IGT', 'IP', 'IPG', 'IBM', 'IFF', 'INTU', 'ISRG', 'IVZ', 'INVH', 'IONS', 'IPGP', 'IQV', 'HPE', 'HXL', 'HIW', 'HRC', 'JAZZ', 'JBHT', 'JBGS', 'JEF', 'JBLU', 'JNJ', 'JCI', 'JLL', 'JPM', 'JNPR', 'KSU', 'KAR', 'K', 'KEY', 'KEYS', 'KRC', 'KMB', 'KIM', 'KMI', 'KEX', 'KLAC', 'KNX', 'KSS', 'KOS', 'KR', 'LB', 'LHX', 'LH', 'LRCX', 'LAMR', 'LW', 'LSTR', 'LVS', 'LAZ', 'LEA', 'LM', 'LEG', 'LDOS', 'LEN', 'LEN.B', 'LII', 'LBRDA', 'LBRDK', 'FWONA', 'IRM', 'ITT', 'JBL', 'JEC', 'LLY', 'LECO', 'LNC', 'LGF.A', 'LGF.B', 'LFUS', 'LYV', 'LKQ', 'LMT', 'L', 'LOGM', 'LOW', 'LPLA', 'LULU', 'LYFT', 'LYB', 'MTB', 'MAC', 'MIC', 'M', 'MSG', 'MANH', 'MAN', 'MRO', 'MPC', 'MKL', 'MKTX', 'MAR', 'MMC', 'MLM', 'MRVL', 'MAS', 'MASI', 'MA', 'MTCH', 'MAT', 'MXIM', 'MKC', 'MCD', 'MCK', 'MDU', 'MPW', 'MD', 'MDT', 'MRK', 'FWONK', 'LPT', 'LSXMA', 'LSXMK', 'LSI', 'CPRI', 'MIK', 'MCHP', 'MU', 'MSFT', 'MAA', 'MIDD', 'MKSI', 'MHK', 'MOH', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 'MORN', 'MOS', 'MSI', 'MSM', 'MSCI', 'MUR', 'MYL', 'NBR', 'NDAQ', 'NFG', 'NATI', 'NOV', 'NNN', 'NAVI', 'NCR', 'NKTR', 'NTAP', 'NFLX', 'NBIX', 'NRZ', 'NYCB', 'NWL', 'NEU', 'NEM', 'NWSA', 'NWS', 'MCY', 'MET', 'MTD', 'MFA', 'MGM', 'JWN', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUS', 'NUAN', 'NUE', 'NTNX', 'NVT', 'NVDA', 'NVR', 'NXPI', 'ORLY', 'OXY', 'OGE', 'OKTA', 'ODFL', 'ORI', 'OLN', 'OHI', 'OMC', 'ON', 'OMF', 'OKE', 'ORCL', 'OSK', 'OUT', 'OC', 'OI', 'PCAR', 'PKG', 'PACW', 'PANW', 'PGRE', 'PK', 'PH', 'PE', 'PTEN', 'PAYX', 'PAYC', 'PYPL', 'NEE', 'NLSN', 'NKE', 'NI', 'NBL', 'NDSN', 'PEP', 'PKI', 'PRGO', 'PFE', 'PCG', 'PM', 'PSX', 'PPC', 'PNFP', 'PF', 'PNW', 'PXD', 'ESI', 'PNC', 'PII', 'POOL', 'BPOP', 'POST', 'PPG', 'PPL', 'PRAH', 'PINC', 'TROW', 'PFG', 'PG', 'PGR', 'PLD', 'PFPT', 'PB', 'PRU', 'PTC', 'PSA', 'PEG', 'PHM', 'PSTG', 'PVH', 'QGEN', 'QRVO', 'QCOM', 'PWR', 'PBF', 'PEGA', 'PAG', 'PNR', 'PEN', 'PBCT', 'RLGY', 'RP', 'O', 'RBC', 'REG', 'REGN', 'RF', 'RGA', 'RS', 'RNR', 'RSG', 'RMD', 'RPAI', 'RNG', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'RGLD', 'RES', 'RPM', 'RSPP', 'R', 'SPGI', 'SABR', 'SAGE', 'CRM', 'SC', 'SRPT', 'SBAC', 'HSIC', 'SLB', 'SNDR', 'SCHW', 'SMG', 'SEB', 'SEE', 'DGX', 'QRTEA', 'RL', 'RRC', 'RJF', 'RYN', 'RTN', 'NOW', 'SVC', 'SHW', 'SBNY', 'SLGN', 'SPG', 'SIRI', 'SIX', 'SKX', 'SWKS', 'SLG', 'SLM', 'SM', 'AOS', 'SJM', 'SNA', 'SON', 'SO', 'SCCO', 'LUV', 'SPB', 'SPR', 'SRC', 'SPLK', 'S', 'SFM', 'SQ', 'SSNC', 'SWK', 'SBUX', 'STWD', 'STT', 'STLD', 'SRCL', 'STE', 'STL', 'STOR', 'SYK', 'SUI', 'STI', 'SIVB', 'SWCH', 'SGEN', 'SEIC', 'SRE', 'ST', 'SCI', 'SERV', 'TPR', 'TRGP', 'TGT', 'TCO', 'TCF', 'AMTD', 'TDY', 'TFX', 'TDS', 'TPX', 'TDC', 'TER', 'TEX', 'TSRO', 'TSLA', 'TCBI', 'TXN', 'TXT', 'TFSL', 'CC', 'KHC', 'WEN', 'TMO', 'THO', 'TIF', 'TKR', 'TJX', 'TOL', 'TTC', 'TSCO', 'TDG', 'RIG', 'TRU', 'TRV', 'THS', 'TPCO', 'TRMB', 'TRN', 'TRIP', 'SYF', 'SNPS', 'SNV', 'SYY', 'DATA', 'TTWO', 'TMUS', 'TFC', 'UBER', 'UGI', 'ULTA', 'ULTI', 'UMPQ', 'UAA', 'UA', 'UNP', 'UAL', 'UPS', 'URI', 'USM', 'X', 'UTX', 'UTHR', 'UNH', 'UNIT', 'UNVR', 'OLED', 'UHS', 'UNM', 'URBN', 'USB', 'USFD', 'VFC', 'MTN', 'VLO', 'VMI', 'VVV', 'VAR', 'VVC', 'VEEV', 'VTR', 'VER', 'VRSN', 'VRSK', 'VZ', 'VSM', 'VRTX', 'VIAC', 'TWLO', 'TWTR', 'TWO', 'TYL', 'TSN', 'USG', 'UI', 'UDR', 'VMC', 'WPC', 'WBC', 'WAB', 'WBA', 'WMT', 'WM', 'WAT', 'WSO', 'W', 'WFTLF', 'WBS', 'WEC', 'WRI', 'WBT', 'WCG', 'WFC', 'WELL', 'WCC', 'WST', 'WAL', 'WDC', 'WU', 'WLK', 'WRK', 'WEX', 'WY', 'WHR', 'WTM', 'WLL', 'JW.A', 'WMB', 'WSM', 'WLTW', 'WTFC', 'WDAY', 'WP', 'WPX', 'WYND', 'WH', 'VIAB', 'VICI', 'VIRT', 'V', 'VC', 'VST', 'VMW', 'VNO', 'VOYA', 'ZAYO', 'ZBRA', 'ZEN', 'ZG', 'Z', 'ZBH', 'ZION', 'ZTS', 'ZNGA', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XPO', 'XYL', 'YUMC', 'YUM']

# Add a few extra tickers (precious metals, international, VIX, blockchain)
Tickers = Tickers + ['GLD', 'FREL', 'VOO', 'TOK', 'PGJ', 'ADRE', 'IPAC', 'SPEU', 'EFAD', 'UIVM', 'SCHC', 'DIM', 'VNQI', 'ILF', 'SDIV', 'XME', 'PICK', 'REMX', 'BATT', 'BLCN']

# Add the nasdaq
Tickers = Tickers + sp.tickers_nasdaq() 

# Alphabetize and remove duplicates
Tickers = sorted(list(set(Tickers)))

# Create a dataframe to populate #
StockInfo = pd.DataFrame()
def build_df():

    # Add the list of tickers to the StockInfo dataframe
    StockInfo['Ticker'] = Tickers
    
    # Set the tickers as the index
    StockInfo.set_index('Ticker', inplace = True)

    # Add some columns that will be used by the functions to determine what needs to be filled #
    for i in Tickers:
        StockInfo.loc[i, 'Ticker'] = np.nan
        StockInfo.loc[i, 'Beta'] = np.nan
        StockInfo.loc[i, 'Dividend Yield'] = np.nan
        StockInfo.loc[i, 'Profit Margin'] = np.nan
        StockInfo.loc[i, 'Earnings Revision Score'] = np.nan
        StockInfo.loc[i, 'Annualized Return (3Y)'] = np.nan
        StockInfo.loc[i, 'Revenue GLY'] = np.nan
        StockInfo.loc[i, 'Sector'] = np.nan

# Create the dataframe
build_df()
StockInfo['Ticker'] = Tickers

# Assign a variable for the downloads path #
download_path = str(os.path.join(Path.home(), "Downloads"))

# Define functions to pull stock data from Yahoo.com #
def page_1():
    for i in Tickers1:
        if pd.isna(StockInfo.loc[i, 'Dividend Yield']):
            try:
                # StockInfo.loc[i, 'Ticker'] = i
                try:
                    # Load the webpage
                    page = requests.get('https://finance.yahoo.com/quote/' +str(i) + '?p=' + str(i) + '&.tsrc=fin-srch')
                    tree = html.fromstring(page.content)
                except:
                    print('connection for {} failed.'.format(i))
                    tree = ''
                    pass
                
                try:
                    StockInfo.loc[i,'PE'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[3]/td[2]/span//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Mkt Cap'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[1]/td[2]/span//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'EPS'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[4]/td[2]/span//text()')[0]
                except: pass
                try:        
                    StockInfo.loc[i,'Dividend Yield'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[6]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Target Price'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[2]/div[2]/table/tbody/tr[8]/td[2]/span//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Spot'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[4]/div/div/div/div[3]/div[1]/div/span[1]//text()')[0]
                    print('page 1 successful for {}.'.format(i))  
                except: 
                    print('page 1 failed for {}.'.format(i))  
                    pass
                
                try:
                    Tickers1.remove(i)
                except:
                    pass
            except:
                print('Closing Loop')
                break
            
        else:
            print(i + ': Complete')
                
    
    try:
        StockInfo['PE'] = StockInfo['PE'].astype(float)
    except: pass
    try:    
        StockInfo['EPS'] = StockInfo['EPS'].astype(float)
    except: pass
    try:    
        StockInfo['Target Price'] = StockInfo['Target Price'].astype(float)
    except: pass
    try:
        StockInfo['Spot'] = StockInfo['Spot'].replace(',','').astype(float)
    except:
        pass
    #===================================#
    for i in StockInfo['Ticker']:
        try:
            if str(StockInfo.loc[i, 'Mkt Cap'])[-1:] == 'M':
                StockInfo.loc[i, 'Mtk Cap'] = float(StockInfo.loc[i, 'Mkt Cap'][:-1]) * 1000000
            elif str(StockInfo.loc[i, 'Mkt Cap'])[-1:] == 'B':
                StockInfo.loc[i, 'Mtk Cap'] = float(StockInfo.loc[i, 'Mkt Cap'][:-1]) * 1000000000
            elif str(StockInfo.loc[i, 'Mkt Cap'])[-1:] == 'T':
                StockInfo.loc[i, 'Mtk Cap'] = float(StockInfo.loc[i, 'Mkt Cap'][:-1]) * 1000000000000
        except:
            pass
        #===================================#
    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)
  
def page_2():
    for i in Tickers2:  
        if pd.isna(StockInfo.loc[i, 'Beta']): 
            try:
                try: 
                    # Load the webpage
                    page = requests.get('https://finance.yahoo.com/quote/' + str(i) + '/key-statistics?p=' + str(i))
                    tree = html.fromstring(page.content)   
                except:
                    print('connection for {} failed.'.format(i))
                    tree = ''
                    pass
                
                try:
                    StockInfo.loc[i,'PEG'] = float(tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[1]/div/div/div/div/table/tbody/tr[5]/td[2]//text()')[0])
                except: pass
                try:
                    StockInfo.loc[i, 'Beta'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[2]/div/div[1]/div/div/table/tbody/tr[1]/td[2]//text()')[0]
                    print('page 2 successful for {}.'.format(i))  
                except: 
                    print('page 2 failed for {}.'.format(i))  
                    pass
                try:
                    StockInfo.loc[i,'Price to Sales'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[1]/div/div/div/div/table/tbody/tr[6]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Price to Book'] = float(tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[1]/div/div/div/div/table/tbody/tr[7]/td[2]//text()')[0])
                except: pass
                try:
                    StockInfo.loc[i,'Enterprise Value/Revenue'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[1]/div/div/div/div/table/tbody/tr[8]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Enterprise Value/EBITDA'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[1]/div/div/div/div/table/tbody/tr[9]/td[2]//text()')[0]
                except: pass
                try: 
                    StockInfo.loc[i,'Profit Margin'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[2]/div/div/table/tbody/tr[1]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Operating Margin'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[2]/div/div/table/tbody/tr[2]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Quarterly Revenue Growth'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[4]/div/div/table/tbody/tr[3]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Quarterly Earnings Growth'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[4]/div/div/table/tbody/tr[8]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Debt to Equity'] = float(tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[5]/div/div/table/tbody/tr[4]/td[2]//text()')[0])
                except: pass
                try:
                    StockInfo.loc[i,'Current Ratio'] = float(tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[5]/div/div/table/tbody/tr[5]/td[2]//text()')[0])
                except: pass
                try:
                    StockInfo.loc[i,'ROA'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[3]/div/div/table/tbody/tr[1]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'ROE'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[3]/div/div[3]/div/div/table/tbody/tr[2]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Percent Insiders'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[2]/div/div[2]/div/div/table/tbody/tr[5]/td[2]//text()')[0]
                except: pass
                try:
                    StockInfo.loc[i,'Percent Institutions'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[2]/div/div[2]/div/div/table/tbody/tr[6]/td[2]//text()')[0]
                except: pass
                try: 
                   StockInfo.loc[i, 'Short Ratio'] = float(tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[2]/div[2]/div/div[2]/div/div/table/tbody/tr[8]/td[2]//text()')[0])
                except: pass
                
                try:
                    Tickers2.remove(i)
                except:
                    pass
            
            except:
                print('Closing Loop')
                break
            
        else:
            print(str(i), ': Complete')

            
      # Format data types
    try:
        StockInfo['Enterprise Value/Revenue'] = StockInfo['Enterprise Value/Revenue'].replace('k','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Beta'] = StockInfo['Beta'].astype(float)
    except:
        pass
    try:
        StockInfo['Enterprise Value/EBITDA'] = StockInfo['Enterprise Value/EBITDA'].replace('k','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Price to Sales'] = StockInfo['Price to Sales'].replace('k','', regex=True).astype(float)    
    except: pass
    try:
        StockInfo['Profit Margin'] = StockInfo['Profit Margin'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Operating Margin'] = StockInfo['Operating Margin'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Quarterly Revenue Growth'] = StockInfo['Quarterly Revenue Growth'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Quarterly Earnings Growth'] = StockInfo['Quarterly Earnings Growth'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['ROA'] = StockInfo['ROA'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['ROE'] = StockInfo['ROE'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:  
        StockInfo['Percent Insiders'] = StockInfo['Percent Insiders'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    try:
        StockInfo['Percent Institutions'] = StockInfo['Percent Institutions'].replace('%','', regex=True).replace(',','', regex=True).astype(float)
    except: pass
    
    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)


def page_3():

    for i in Tickers3: 
        if pd.isna(StockInfo.loc[i, 'Earnings Revision Score']):            
            try:
                
                # Load the webpage
                page = requests.get('https://finance.yahoo.com/quote/' + str(i) + '/analysis?p=' + str(i))
                tree = html.fromstring(page.content)   
                print('page 3 successful for {}.'.format(i))
                
            except:
                print('page 3 failed for {}.'.format(i))
                tree=''
                pass
            
            # Pull in the earnings estimates
            try:
                CurrentQuarterRevisions = float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[2]/span//text()')[0])/(float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[2]/span//text()')[0])+float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[4]/td[2]/span//text()')[0]))*10
            except: pass
            try:
                NextQuarterRevisions = float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[3]/span//text()')[0])/(float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[3]/span//text()')[0])+float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[4]/td[3]/span//text()')[0]))*10
            except: pass
            try:
                CurrentYearRevisions = float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[4]/span//text()')[0])/(float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[4]/span//text()')[0])+float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[4]/td[4]/span//text()')[0]))*10
            except: pass
            try:
                NextYearRevisions = float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[5]/span//text()')[0])/(float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[2]/td[5]/span//text()')[0])+float(tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[5]/tbody/tr[4]/td[5]/span//text()')[0]))*10                
            except: pass
            try:
                StockInfo.loc[i,'Earnings Revision Score'] = CurrentQuarterRevisions + NextQuarterRevisions + CurrentYearRevisions + NextYearRevisions
            except:
                pass
            
            # Pull in the sales growth estimates
            try:
                StockInfo.loc[i, 'Sales Growth Est Curr Q'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[6]/tbody/tr[1]/td[2]//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'Sales Growth Est Next Q'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[6]/tbody/tr[2]/td[2]//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'Sales Growth Est Curr Y'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[6]/tbody/tr[3]/td[2]//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'Sales Growth Est Next Y'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[6]/tbody/tr[4]/td[2]//text()')[0]).replace('%',''))
            except: pass
            try:                
                StockInfo.loc[i, 'Sales Growth Est Next 5Y'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[6]/tbody/tr[5]/td[2]//text()')[0]).replace('%',''))
            except: pass
            
                
            # Pull in the last 4 earnings surprises
            try:
                StockInfo.loc[i, 'T0 Earnings Surprise'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[3]/tbody/tr[4]/td[5]/span//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'T-1 Earnings Surprise'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[2]/tbody/tr[6]/td[4]/span//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'T-2 Earnings Surprise'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[2]/tbody/tr[6]/td[3]/span//text()')[0]).replace('%',''))
            except: pass
            try:
                StockInfo.loc[i, 'T-3 Earnings Surprise'] = float((tree.xpath('//*[@id="Col1-0-AnalystLeafPage-Proxy"]/section/table[2]/tbody/tr[6]/td[2]/span//text()')[0]).replace('%',''))
            except: pass
            
            try:
                Tickers3.remove(i)
            except:
                pass
            
        else:
            print(i + ': Complete')

    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)
        
        

# Pull stock returns and calculate deviation, average returns, sharpe and Treynor ratio #
def page_4():
    for i in Tickers4:
        if pd.isna(StockInfo.loc[i, 'Annualized Return (3Y)']):
            try:
                try:
                    #===========================================================================#
                    prices = web.DataReader(i, start='2019-01-01', end = today, data_source='yahoo')
                    price_data = []
                    price_data.append(prices.assign(ticker=i)[['Adj Close']])
                    # remaining_tickers.append(tickers[ticker])
                    Volatility = pd.DataFrame(price_data[0])
                    Volatility = Volatility.tail(756)
                    #===================================#
                    Volatility['Daily_Dev'] = Volatility['Adj Close'].pct_change()
                    Volatility = Volatility.dropna()
                    Annual_Deviation = np.std(Volatility['Daily_Dev'])*np.sqrt(756)
                    #===================================#
                    Annual_Return = ((float(Volatility['Adj Close'].tail(1))-float(Volatility['Adj Close'].head(1)))/float(Volatility['Adj Close'].head(1)))/3
                    #===================================#
                    Sharpe = (Annual_Return - Risk_Free_Rate)/Annual_Deviation
                    #===================================#
                    Treynor = Annual_Return - Risk_Free_Rate
                    #===================================#
                    StockInfo.loc[i, 'Annualized Return (3Y)'] = Annual_Return
                    StockInfo.loc[i, 'Annualized Deviation (3Y)'] = Annual_Deviation
                    StockInfo.loc[i, 'Sharpe'] = Sharpe
                    print('page 4 successful for {}.'.format(i))
                except:
                    print('page 4 failed for {}.'.format(i))
                    pass
                
                try:
                    StockInfo.loc[i, 'Treynor'] = Treynor/StockInfo.loc[i, 'Beta']
                except:
                    pass
                
                try:
                    Tickers4.remove(i)
                except:
                    pass
            
            except:
                print('Closing Loop')
                break
            
        else:
            print('Page 4 for {} is complete.'.format(i))
            pass      
            
    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)
            

def page_5():
    failures = 0
    for i in Tickers5: 
        if pd.isna(StockInfo.loc[i, 'Revenue GLY']):
            time.sleep(5)
            # This IF statement is for error handling. Sometimes it fails and just needs some time to rest before it can work again. IDK why.
            if failures > 4:
                time.sleep(120)  
                failures = 0
            else:             
                try:
                    income_statement = sp.get_income_statement(i)
                    income_statement.reset_index(inplace = True)
                
                    # Calculate growth from income statements - GLY indicates "Growth Last Year"
                    try: 
                        StockInfo.loc[i, 'Revenue GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Revenue GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Total Revenue'].iloc[:,5])[0]))
                        failures = 0
                        Tickers5.remove(i)
                        print('page 5 successful for {}.'.format(i))
                    except:
                        print('page 5 failed for {}.'.format(i))
                        pass
                        failures = failures + 1
                    try:
                        StockInfo.loc[i, 'Cost of Revenue GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Cost of Revenue GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Cost of Revenue'].iloc[:,5])[0]))
                    except:
                        pass
                    try:   
                        StockInfo.loc[i, 'Gross Profit GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Gross Profit GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Gross Profit'].iloc[:,5])[0]))
                    except:
                        pass
                    try:   
                        StockInfo.loc[i, 'Operating Expense GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Operating Expense GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Operating Expense'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'Operating Income GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Operating Income GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Operating Income'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'Net Income GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Net Income GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Net Income Common Stockholders'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'Basic EPS GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Basic EPS GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Basic EPS'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'Diluted EPS GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Diluted EPS GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Diluted EPS'].iloc[:,5])[0]))
                    except:
                        pass
                    try: 
                        StockInfo.loc[i, 'EBIT GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'EBIT GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'EBIT'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'EBITDA GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'EBITDA GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'EBITDA'].iloc[:,5])[0]))
                    except:
                        pass
                    try:
                        StockInfo.loc[i, 'Normalized EBITDA GLY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,3])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,3])[0]))
                        StockInfo.loc[i, 'Normalized EBITDA GTY']  = (float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,2])[0]) - float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,5])[0])) / abs(float(list(income_statement[income_statement['Breakdown'] == 'Normalized EBITDA'].iloc[:,5])[0]))
                    except:
                        pass
                except:
                    pass
            
        else:
            print('Page 5 for {} is complete.'.format(i))
            pass
    
    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)


def page_6():
    
    for i in Tickers6:  
        if pd.isna(StockInfo.loc[i, 'Sector']):
            time.sleep(3)
            try:
                
                # Load the webpage
                page = requests.get('https://finance.yahoo.com/quote/' +i + '/profile?p=' +i)
                tree = html.fromstring(page.content)
                
            except:
                print('connection for {} failed.'.format(i))
                tree = ''
                pass
                
            try:
                StockInfo.loc[i,'Sector'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[1]/div/div/p[2]/span[2]//text()')[0]
                print('page 6 successful for {}.'.format(i))
            except:
                print('page 6 failed for {}.'.format(i))
                pass
            try:
                StockInfo.loc[i,'Industry'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/div[1]/div/div/p[2]/span[4]//text()')[0]
            except:
                pass
            try:
                StockInfo.loc[i,'Description'] = tree.xpath('/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/section[2]/p//text()')[0]
            except:
                pass
            
            try:
                Tickers6.remove(i)
            except:
                pass
        else:
            print('Page 6 for {} is complete.'.format(i))
            pass
          
    with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
        StockInfo.to_excel(writer, sheet_name='Stats', index = False)
        
            
#=========================================================================================================================#

''' Monte Carlo '''

# This runs a simulation over a specified time period where random returns are generated for each year #
def FutureValue(years, avg, sd, value):
    
    # Set the initial future_value variable equal to the starting value provided in the function #
    future_value = float(value)
    for i in range(years):
        
        # Return a random variable based on the avg & sd given in the function - repeat this for each year #
        ret = random.gauss(avg, sd)
        if ret <= -1:
            ret = -.9999
        
        # Compound the future_value by multiplying it by the randomly generated return from above #        
        ret = (1+ret)
        future_value = float(future_value) * float(ret)
    return future_value

# The simulation runs 10000 times #
def Monte_Carlo():
    
    for i in Tickers:
        
        print('simulating {}'.format(i))
        outcome = []
        for num in range(10000):
            
                # This is a test using NFLX #
            total_return = FutureValue(1, 
                                       StockInfo.loc[i, 'Annualized Return (3Y)'], 
                                       StockInfo.loc[i, 'Annualized Deviation (3Y)'],
                                       StockInfo.loc[i, 'Spot'])
        
        # Append the final value of each simulation to a list that stores them all #
            outcome.append(total_return)
        
        try:
            # Just print everything so that it looks pretty #
            StockInfo.loc[i, 'MC Mean Return'] = round((float(statistics.mean(outcome)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # Mean Return #
            StockInfo.loc[i, 'MC Max Loss'] = round((float(min(outcome)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # Max Loss #
            StockInfo.loc[i, 'MC Max Return'] = round(float((max(outcome)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # Max Return #
            StockInfo.loc[i, 'MC 25th Percentile'] = round((float(np.percentile(outcome, 25)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # 25th Percentile Return #
            StockInfo.loc[i, 'MC 50th Percentile'] = round((float(np.percentile(outcome, 50)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # 50th Percentile Return #
            StockInfo.loc[i, 'MC 75th Percentile'] = round((float(np.percentile(outcome, 75)) - float(StockInfo.loc[i,'Spot']))/float(StockInfo.loc[i,'Spot']), 2) # 75th Percentile Return #
        except:
            pass
        
#=========================================================================================================================#  

''' Run the extraction functions '''


Tickers1 = list(StockInfo['Ticker'])
Tickers2 = list(StockInfo['Ticker'])
Tickers3 = list(StockInfo['Ticker'])
Tickers4 = list(StockInfo['Ticker'])
Tickers5 = list(StockInfo['Ticker'])
Tickers6 = list(StockInfo['Ticker'])

for i in range(10):
    page_1()
    time.sleep(10)
    page_2()
    time.sleep(10)
    page_3()
    time.sleep(10)
    page_4()
    time.sleep(180)
    page_5()
    time.sleep(10)
    page_6()
    time.sleep(10)

#=========================================================================================================================#   

''' Data cleanup of latest pull '''

# Remove text from integers #
StockInfo = StockInfo.replace('N/A', np.nan)
for i in list(StockInfo):
    try:
        StockInfo[i] = StockInfo[i].str.replace(',','').str.replace('%','').str.replace('k','').astype(float)
        print(i)
    except:
        pass
StockInfo['Price to Sales'] = StockInfo['Price to Sales'].replace('k','').astype(float)
StockInfo['Enterprise Value/Revenue'] = StockInfo['Enterprise Value/Revenue'].replace('k','').astype(float)
StockInfo['Enterprise Value/EBITDA'] = StockInfo['Enterprise Value/EBITDA'].replace('k','').astype(float)
StockInfo['Profit Margin'] = StockInfo['Profit Margin'].replace('%','').replace('k','')
StockInfo['Profit Margin'] = StockInfo['Profit Margin'].replace('%','').replace(',','').astype(float)
StockInfo['Quarterly Revenue Growth'] = StockInfo['Quarterly Revenue Growth'].replace('%','').replace('k','')
StockInfo['Quarterly Revenue Growth'] = StockInfo['Quarterly Revenue Growth'].replace(',','').astype(float)
StockInfo['Quarterly Earnings Growth'] = StockInfo['Quarterly Earnings Growth'].replace('%','').replace('k','')
StockInfo['Quarterly Earnings Growth'] = StockInfo['Quarterly Earnings Growth'].replace('%','').replace(',','').astype(float)
StockInfo['ROA'] = StockInfo['ROA'].replace('%','').replace('k','').astype(float)
StockInfo['ROE'] = StockInfo['ROE'].replace('%','').replace('k','')
StockInfo['ROE'] = StockInfo['ROE'].replace('%','').replace(',','').astype(float)
StockInfo['Percent Insiders'] = StockInfo['Percent Insiders'].replace('%','').replace('k','')
try:
    StockInfo['Percent Insiders'] = StockInfo['Percent Insiders'].str.replace('%','').str.replace(',','').astype(float)
except:
    pass
StockInfo['Percent Institutions'] = StockInfo['Percent Institutions'].replace('%','').replace('k','')
StockInfo['Percent Institutions'] = StockInfo['Percent Institutions'].replace('%','').replace(',','').astype(float)

# Drop any target price values that accidentally pulled in date values
for i in Tickers:
    if str(StockInfo.loc[i, 'Target Price']).find('-') > 0:
        StockInfo.loc[i, 'Target Price'] = np.nan

 # Calculate projected growth #       
StockInfo['Projected Growth'] = (StockInfo['Target Price'].astype(str).str.replace(',','').astype(float)/(StockInfo['Spot'].astype(str).str.replace(',','')).astype(float))-1

# Convert the dividend yield to a whole number and replace NULL values with 0 #
StockInfo['Dividend Yield'] = StockInfo['Dividend Yield'].astype(str).apply(lambda st: st[st.find("(")+1:st.find(")")])
StockInfo['Dividend Yield'] = StockInfo['Dividend Yield'].str.replace('%', '')
StockInfo['Dividend Yield'] = StockInfo['Dividend Yield'].str.replace('N/A', '0')
StockInfo['Dividend Yield'] = StockInfo['Dividend Yield'].str.replace('na', '0')
StockInfo['Dividend Yield'] = StockInfo['Dividend Yield'].astype(float)
 
# Data cleanup #
StockInfo['Price to Sales'] = StockInfo['Price to Sales'].replace('k','').astype(float)
StockInfo['Enterprise Value/Revenue'] = StockInfo['Enterprise Value/Revenue'].replace('k','').astype(float)
StockInfo['Enterprise Value/EBITDA'] = StockInfo['Enterprise Value/EBITDA'].replace('k','').astype(float)
StockInfo['Profit Margin'] = StockInfo['Profit Margin'].replace('%','').replace('k','')
StockInfo['Profit Margin'] = StockInfo['Profit Margin'].replace(',','').astype(float)
StockInfo['Operating Margin'] = StockInfo['Operating Margin'].replace('%','').replace('k','')
StockInfo['Operating Margin'] = StockInfo['Operating Margin'].astype(str).str.replace(',','')
StockInfo['Quarterly Revenue Growth'] = StockInfo['Quarterly Revenue Growth'].replace('%','').replace('k','')
StockInfo['Quarterly Revenue Growth'] = StockInfo['Quarterly Revenue Growth'].replace(',','').astype(float)
StockInfo['Quarterly Earnings Growth'] = StockInfo['Quarterly Earnings Growth'].replace('%','').replace('k','')
StockInfo['Quarterly Earnings Growth'] = StockInfo['Quarterly Earnings Growth'].replace(',','').astype(float)
StockInfo['ROA'] = StockInfo['ROA'].replace('%','').replace('k','').astype(float)
StockInfo['ROE'] = StockInfo['ROE'].replace('%','').replace('k','')
StockInfo['ROE'] = StockInfo['ROE'].replace(',','').astype(float)
try:
    StockInfo['Percent Insiders'] = StockInfo['Percent Insiders'].replace('%','').replace('k','')
    StockInfo['Percent Insiders'] = StockInfo['Percent Insiders'].replace(',','').astype(float)
except:
    pass
StockInfo['Percent Institutions'] = StockInfo['Percent Institutions'].replace('%','').replace('k','')
StockInfo['Percent Institutions'] = StockInfo['Percent Institutions'].replace(',','').astype(float)

# Clean up the Latest Spot price data #
StockInfo['Spot'] = StockInfo['Spot'].dropna()
StockInfo = StockInfo[StockInfo['Spot'] != 'nan']
StockInfo = StockInfo[StockInfo['Spot'] != '']
try:
    StockInfo['Spot'] = StockInfo['Spot'].str.replace(',','')
except:
    pass

# Run the Monte Carlo 
Monte_Carlo()

#=========================================================================================================================#

# Export #
with pd.ExcelWriter(download_path + '\\Stock Rankings ' + str(today) + '.xlsx') as writer:
    StockInfo.to_excel(writer, sheet_name='Stats', index = False)