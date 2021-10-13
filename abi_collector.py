import urllib.request as ul
from bs4 import BeautifulSoup
import json
import requests
import pandas as pd
from lxml import etree
import os

# ## Test addresses
# unknown? Congress
contract_congress = '0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359'
# highflyers
contract_highflyers = '0x248F0913387eEf740B1DC73Ba81c815B748a8859'
# opensea
contract_opensea = '0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b'
# crypto_kitties
contract_crypto_kitties = '0x06012c8cf97bead5deae237070f9587f8e7a266d'

contract_str, contract = 'opensea', contract_opensea

# use by get_ABI_bytecode
BASE_URL = 'https://etherscan.io/address/'
DATA_FILENAME = '/home/forsagedata/NFT-study/nate/contract_abi.csv'
columns = ['contract_name', 'contract_address', 'abi', 'abi_bytecode']


def get_ABI_only(contract_address : str):
    """
    input var: full contract address including 0x
    output: abi as a dict
    """
    url = f'https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}'
    req = ul.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    client = ul.urlopen(req)
    htmldata = client.read()
    data_dict = json.loads(htmldata.decode('utf-8'))

    client.close()    
    return data_dict['result']


def get_ABI_bytecode(contract_address : str) -> 'abi_bytecode':
    """
    input var: full contract address including 0x
    output: abi as a dict, abi in bytecode
    """
    url = f'{BASE_URL}{contract_address}#code'
    req = ul.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    client = ul.urlopen(req)
    htmldata = client.read()

    soup = BeautifulSoup(htmldata, 'html.parser')

    bytecode = soup.find("div", { "id": "verifiedbytecode2" }).text
    return bytecode

    # return bytecode


def get_ABI(contract_name : str,contract_address : str,DATA_FILENAME : str):
    """
    input: contract_name (used to store in file), contract_address, filename - which file to append to
    output: 
    """
    
    abi_ = get_ABI_only(contract_address)
    abi_bytecode = get_ABI_bytecode(contract_address)

    df = pd.DataFrame([[contract_name, contract_address, abi_, abi_bytecode]], columns=columns)

    if os.path.exists(DATA_FILENAME):
        file_df = pd.read_csv(DATA_FILENAME, index_col=0)
        file_df = file_df.append(df, ignore_index=True)
    file_df.to_csv(DATA_FILENAME, mode='w', header=columns)

    return df


def check_entry(DATA_FILENAME : str, contract_address : str) -> (bool, 'pd.row'):
    """
    input: data file name (ensure exact location), contract name (as written in etherscan)
    output: true - if exists in file, else - false. returns row with following columns "contract_name", "contract_address", "abi", "abi_bytecode"
    """
    if not os.path.exists(DATA_FILENAME):
        print(f'file does not exist creating file: {DATA_FILENAME}')

    # check that file is built correctly
    else:
        df = pd.read_csv(DATA_FILENAME, index_col=0)
        if not ('contract_name' in df.columns and 'contract_address' in df.columns and 'abi' in df.columns and 'abi_bytecode' in df.columns):
            print(f'file has incorrect columns, requies; {columns}')
            print(f'file has columns: {df.columns}')
            exit()
    
        # check if entry exists
        else:
            row = df.loc[df['contract_address'].str.contains(contract_address)]
            row = pd.DataFrame(row, columns=columns)
            if (len(row['contract_name']) != 0):
                return True, row
            # for idx, row in df.iterrows():
            #     if row[1] == contract_address:
            #         return True, row
    return False, "entry does not exist"
    
    # print(df)

def contract_abi(contract_address: str, nft_name: str) -> dict:
    """
    used to call from other files
    input: nft contract address
    output1: dict -> ["index_in_file", "contract_name", "contract_address", "abi", "abi_bytecode"]
    output2: list of dict converted to string values, used by 'w3.eth.contract(abi=abi, bytecode=bytecode)' function
    """
    check, row = check_entry(DATA_FILENAME, contract_address)

    # if entry not in file, get it from etherscan
    if not check:
        print('entry not in file, retriveing from etherscan...')
        row = get_ABI(nft_name, contract_address, DATA_FILENAME)

    contract_name = row['abi'].values.tolist()
    contract_name_string = " ".join(contract_name)

    contract_address = row['abi'].values.tolist()
    contract_address_string = " ".join(contract_address)

    abi = row['abi'].values.tolist()
    abi_string = " ".join(abi)

    bytecode = row['abi_bytecode'].values.tolist()
    bytecode_string = " ".join(bytecode)

    return row, [contract_name_string, contract_address_string, abi_string, bytecode_string]         


if __name__ == '__main__':

    contract = input('enter contract address: ').lower()

    check, row = check_entry(DATA_FILENAME, contract)

    # if entry not in file, get it from etherscan
    if not check:
        contract_str = input('enter contract name: ')
        print('entry not in file, retriveing from etherscan...')
        get_ABI(contract_str, contract, DATA_FILENAME)

    else:
        print(f'entry on file:')
        print(row)
    