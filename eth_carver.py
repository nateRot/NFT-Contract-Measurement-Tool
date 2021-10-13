from web3 import Web3
import abi_collector
import numpy as np
import pandas as pd
import math
import sys
from decorators import timeit
from tqdm import tqdm
import ast
ether = lambda value: float(value) / 1e18

contract_name = 'opensea'
contract_address = '0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b'.lower()


FILE_NAME = ''

w3 = Web3(Web3.HTTPProvider('http://localhost:8646'))




def get_rows(contract_address: str, file_name: str, receipts_filename: str) -> pd.DataFrame():
    """
    used to get rows from file genereated by ethereumetl (tokens csv) with contract address in 'to_address' field
    gets rows of success status
    input1: contract address
    input2: full file name
    output: returns pandas df with requested rows
    """
    receipts_df = pd.read_csv(receipts_filename)
    receipts_df = receipts_df.sort_values(['block_number', 'transaction_index'], ignore_index=True)
    df = pd.read_csv(file_name)
    df = df.sort_values(['block_number', 'transaction_index'], ignore_index=True)

    df = df.loc[receipts_df['status'] == 1] 
    df = df.loc[df["to_address"] == contract_address.lower()]
    df = df.reset_index()
    df = df.drop(columns='index')
    return df


def add_function_input(df: pd.DataFrame(), eth_contract: w3.eth.contract(), func_list : list, function_hist: dict, bad_list: list) -> pd.DataFrame():
    """
    adds column to dataframe of function called on contract, inputed into column as dictionary
    prints number of each type of function found
    input1: dataframe of rows to_address of contract_address
    *important* does not check if rows are correct, user must ensure
    input2: eth_contract the contract 'translator'
    input3: func_list - a list of all functions to select
    input4: funciton_hist - empty dictionar, will return number of each function in file
    input5: bad_list - empty list, 
    output: dataframe including added function_call column
    """
    function_list = []
    func_name_list = []
    token_id_list = []
    function_dict = {}
    plain_text_dict = {}

    for idx, row in df.iterrows():
        # checks to ensure input for function is valid, else drops from data
        try:
            function = eth_contract.decode_function_input(row['input'])
        except:
            bad_list.append(idx)
            df = df.drop(index=idx)
            continue
        func_name = function[0].function_identifier
        func_name_list.append(func_name)
        if func_name in func_list:
            # TODO change depending on contract
            try:
                token_id_list.append(function[1]['tokenId'])
            except:
                try:
                    token_id_list.append(function[1]['_tokenId'])
                except:
                    token_id_list.append('')
        else:
            token_id_list.append('')
        if func_name in function_hist:
            function_hist[func_name] += 1
        else:
            function_hist[func_name] = 1
        function_dict.update(function[1])
        function_list += [function_dict]
    df['token_id'] = token_id_list
    df['function_name'] = func_name_list
    df['function_call'] = function_list
    
    return df


def opensea_add_function_input(df: pd.DataFrame(), eth_contract: w3.eth.contract(), func_list : list, function_hist: dict, bad_list: list) -> pd.DataFrame():
    """
    used for OPENSEA transactions
    adds column to dataframe of function called on contract, inputed into column as dictionary
    prints number of each type of function found
    input1: dataframe of rows to_address of contract_address
    *important* does not check if rows are correct, user must ensure
    input2: eth_contract the contract 'translator'
    input3: func_list - a list of all functions to select
    input4: funciton_hist - empty dictionar, will return number of each function in file
    input5: bad_list - empty list, 
    output: dataframe including added function_call column
    """
    trans_contract_address_list = []
    token_id_list = []
    function_list = []
    func_name_list = []
    price_list = []
    seller_list = []
    buyer_list = []
    contract_standard_list = []
    function_dict = {}
    plain_text_dict = {}
    num_dropped = 0

    for idx, row in df.iterrows():
        # checks to ensure input for function is valid, else drops from data

        try:
            function = eth_contract.decode_function_input(row['input'])
        except:
            num_dropped += 1
            bad_list.append(idx)
            df = df.drop(index=idx)
            continue

        for item in function[1]['rssMetadata']:
            # item = ast.literal_eval(item)
            item = item.split(b'#')
            item = int.from_bytes(item, "big")

        # drops all functions except
        if function[0].function_identifier != 'atomicMatch_':
            num_dropped += 1
            df = df.drop(index=idx)
            continue
        sub_nft_contract = function[1]['addrs'][4].lower()
        trans_contract_address_list.append(sub_nft_contract)

        # collect only ether or wrapped either values
        if function[1]['addrs'][6].lower() in ['0x0000000000000000000000000000000000000000', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'.lower()]:
            price_list.append(ether(function[1]['uints'][4]))
        else:
            price_list.append('alt_coin')

        # get the contract standard
        contract_standard_input = row['input'][64*53:64*55].strip('0')
        if contract_standard_input == 'c4f242432a':
            contract_standard = 1155
        elif contract_standard_input == '6423b872dd':
            contract_standard = 721
        else:
            contract_standard = None
        contract_standard_list.append(contract_standard)


        if function[1]['feeMethodsSidesKindsHowToCalls'][1] == 0:
            seller_list.append(function[1]['addrs'][2].lower())
            buyer_list.append(function[1]['addrs'][1].lower())
        else:
            seller_list.append(function[1]['addrs'][1].lower())
            buyer_list.append(function[1]['addrs'][2].lower())
        calldataBuy = function[1]['calldataBuy']
        func_name_list.append(function[0].function_identifier)

        if func_name_list[-1] in func_list:
            trans_token_list = []
            # special case for Opensea shared storefront
            if (sub_nft_contract == '0x495f947276749ce646f68ac8c248420045cb7b5e'.lower()):
                token_hex = row['input'][64*64:64*66]
                # convert hexa to decimal
                token_hex = int(token_hex.strip("0"), 16)
                trans_token_list.append(token_hex)
                token_id_list.append(trans_token_list)
            else:
                # end of 100 bytes, ending of token ID in bytes
                string_list = function[1]['calldataSell'].split(b'#')
                for i in range(1, len(string_list)):
                    string = string_list[i][:100]
                    if (i == len(string_list) - 1) and (len(string_list) > 2):
                        string = string[:99]
                    # length of token ID is exactly 32 bytes long
                    token_hex = string[-32:]
                    trans_token_list.append(int.from_bytes(token_hex, "big"))
                token_id_list.append(trans_token_list)

        else:
            token_id_list.append('')
        if func_name_list[-1] in function_hist:
            function_hist[func_name_list[-1]] += 1
        else:
            function_hist[func_name_list[-1]] = 1
        function_dict.update(function[1])
        function_list += [function_dict] 
    df['function_name'] = func_name_list
    df['function_call'] = function_list
    df['price'] = price_list
    df['seller'] = seller_list
    df['buyer'] = buyer_list
    df['contract_standard'] = contract_standard_list
    df['nft_address'] = trans_contract_address_list
    df['token_id'] = token_id_list
    return df


def main_looper(df: pd.DataFrame()) -> list:
    """
    loop doing most of the grunt work on **Individual NFT contracts** not opensea
    - goes through the dataframe, creating a match between requests to sell and a succesful sell
    uses a dictionary (hash table) to keep it organized where an entry is the token ID as key, value being the time
    - for_sale_dictionary holds time requested to sell, value reset to -1 once transfer for token ID has occured
    - sell_dictionary holds 'token ID + index sold' (i.e first time sold index sold = 1), and interval between posted for sale
    and transfer taken place (liquidity)
    - repeat_sale dictionary is used to track number of times a token is attempted to sell without success, first attempt is recorded
    for interval
    """
    for_sale_dictionary = {}
    sell_dictionary = {}
    repeat_sale = {}
    unrecorded_sale = []
    for idx, row in df.iterrows():
        if row['function_call']['function'] == 'createSaleAuction':
            # TODO - change for different NFT
            # entry into dict is kitty ID (unique to cryptokitties)
            # checks if already posted to be sold, continues if this is a reentry to sell, recording it
            if row['function_call']['_kittyId'] in for_sale_dictionary:
                if row['function_call']['_kittyId'] in repeat_sale:
                    repeat_sale[row['function_call']['_kittyId']] += 1
                    continue
                else:
                    repeat_sale[row['function_call']['_kittyId']] = 1 
                    continue
            for_sale_dictionary.update({str(row['function_call']['_kittyId']) : row['block_timestamp']})
            
        if row['function_call']['function'] == 'transfer':
            # must have posted for sale in an earlier block, cant find interval
            if not row['function_call']['_tokenId'] in for_sale_dictionary:
                unrecorded_sale.append([row['hash'], row['function_call']['_tokenId']])
                # print(f"token not recoreded for sale: {row['function_call']['_tokenId']}")
                # print(row['hash'])
                continue
            if for_sale_dictionary[row['function_call']['_tokenId']] == -1:
                print('missed for sale block, somehow??')
                continue
            # TODO find a smarter way to track times transfered!!
            sell_entry = str(row['function_call']['_tokenId']) + '_' + str(row['block_number'])
            print('')
            print('')
            print('')
            print(row['block_timestamp'])
            print(for_sale_dictionary['_tokenId'])
            time_interval = row['block_timestamp'] - for_sale_dictionary['_tokenId']
            for_sale_dictionary[row['function_call']['_tokenId']] = -1
            print(time_interval)
            sell_dictionary.update({sell_entry : row['block_timestamp']})
    print(len(unrecorded_sale))
    if len(unrecorded_sale) > 0:
        rng = min(7, len(unrecorded_sale))
        for ii in range(rng):
            print(unrecorded_sale[ii])
        for jj in range(rng):
            print(unrecorded_sale[len(unrecorded_sale)-jj-1])
            

    # print(f'Token IDs posted for sale:')
    # for key in for_sale_dictionary:
    #     print(f'{key} : {for_sale_dictionary[key]}')

@timeit
def clean_csv(read_file: str, output_file: str,  nft_contract, opensea=False, inculde_funcs = ['atomicMatch_'], chunksize = 100000):
    """
    collects only 'atomicMatch_', 'transfer' transactions that were completed successfully

    interesting functions to consider: ['createSaleAuction', 'transfer', 'atomicMatch_', 'transferFrom']

    if 'opensea' set to true the read_file csv is regarded as a file with only opensea transactions, in which case will parse for each transaction 
    in accordance with the nft contract interacted with by the opensea contract interacted. collect atomicMatch_ functions

    """

    f = open(output_file, 'w')
    f.write('hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input,block_timestamp,max_fee_per_gas,max_priority_fee_per_gas,transaction_type,function_name,function_call,price,seller,buyer,contract_standard,nft_address,token_id\n')
    f.close()
    chunk_num = 0
    function_hist = {}
    bad_list = []
    num_lines = sum(1 for line in open(read_file))
    with tqdm(total=(math.ceil(num_lines/chunksize)), desc='retrieving functions using ABI', ) as pbar:
        for chunk in pd.read_csv(read_file, chunksize=chunksize):
            chunk_num += 1
            if opensea:
                df = opensea_add_function_input(chunk, nft_contract, ['atomicMatch_'], function_hist, bad_list)
            else:
                df = add_function_input(chunk, nft_contract, inculde_funcs, function_hist, bad_list)
            if len(inculde_funcs) > 0:
                df = df[df['function_name'].isin(inculde_funcs)]
            
            df.to_csv(output_file, mode='a', index=False, header=False, columns=df.columns)
            pbar.update(1)

    print(f'functions found: {function_hist}')
    print(f'number of bad function calls: {len(bad_list)}')
    return df.columns


def wrapper_complete_run(FILE_NAME: str, contract_address, contract_name, opensea=False):
    """
    used by complete_pipeline_execution.py
    """
    w3 = Web3(Web3.HTTPProvider('http://localhost:8646'))

    contract_abi_dict, contract_abi_list = abi_collector.contract_abi(contract_address, contract_name)
    abi = contract_abi_list[2]
    bytecode = contract_abi_list[3]

    nft_contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    header = clean_csv(FILE_NAME,f'data/clean_{contract_name}.csv', nft_contract, inculde_funcs=[], opensea=opensea, chunksize=100000)
    print()
    print('**** Completed CARVER element ****')
    print()

if __name__ == '__main__':
    contract_abi_dict, contract_abi_list = abi_collector.contract_abi(contract_address, contract_name)
    abi = contract_abi_list[2]
    bytecode = contract_abi_list[3]

    nft_contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    header = clean_csv(FILE_NAME,f'data/clean_{contract_name}.csv', nft_contract, opensea=True, chunksize=100000)



    

    


