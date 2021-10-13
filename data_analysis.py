from web3 import Web3
import abi_collector
import numpy as np
import pandas as pd
from decorators import timeit
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
from matplotlib.ticker import OldScalarFormatter
from collections import defaultdict
import datetime
import math
import ast

import urllib.request as ul
from bs4 import BeautifulSoup
import json
import requests
from lxml import etree
import os
import re

ether = lambda value: float(value) / 1e18

pd.set_option("display.max_rows", 20, "display.max_columns", None,'display.max_colwidth', None)

def round_nearest_mag(x, mag):
    return int(math.ceil(x / mag)) * mag


BASE_URL = 'https://etherscan.io/token/'

# file output from eth_csv_carver.py
# headers in file should be [hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input,block_timestamp,max_fee_per_gas,max_priority_fee_per_gas,transaction_type,token_id,function_name,function_call]
CARVED_FILE = 'clean_crypto_kitties.csv'
plots_folder = 'nft_plots/'


def times_func_called_on_token(filename: str, output_plot_file: str, called_func: str, plot=False) -> dict:
    trans_dict = defaultdict(int)
    for chunk in pd.read_csv(filename, chunksize=100000, low_memory=False, dtype=str):
        df = chunk.loc[chunk['function_name'] == called_func]
        for idx, row in df.iterrows():
            for token in ast.literal_eval(row['token_id']):
                trans_dict[token] += 1
    print('top most traded amounts are:')
    print('token ID ***||*** Number of trades')
    reduced_dict = trans_dict.copy()
    for i in range(10):
        if (len(reduced_dict) == 0):
            break
        key = max(reduced_dict, key=reduced_dict.get)
        print(f'{key} : {reduced_dict[key]}')
        reduced_dict.pop(key, None)
    if plot:
        plt.figure(figsize=(15,5))
        plt.bar(trans_dict.keys(), trans_dict.values(), color='g')
        plt.savefig(output_plot_file)
    return trans_dict


def func_call_bin_histogram(nft_contract: str, filename: str, output_plot_file: str, called_func: str, plot=False, graph_type='sale') -> dict:
    trans_dict = times_func_called_on_token(filename, output_plot_file, called_func, plot=False)
    total_transactions = sum(trans_dict.values())
    bin_dict = defaultdict(int)
    for key, item in trans_dict.items():
        bin_dict[item] += 1
    max_traded_token = max(bin_dict, key=bin_dict.get)

    if plot:
        total_tokens = get_total_tokens(nft_contract)

        fig = plt.figure(figsize=[16,8])
        ax = fig.add_subplot()
        ax.hist(trans_dict.values(), bins=list(range(0, int(round_nearest_mag(max(bin_dict), 10)))), log=True)
        # ax.semilogy()
        ax.yaxis.set_major_formatter(OldScalarFormatter())
        ax.grid()
        ax.set_xlim([0,round_nearest_mag(max(bin_dict), 10)])
        ax.set_xlabel(f'number of {graph_type}s')
        ax.set_ylabel(f'number of tokens')
        ax.set_title(f'How frequent are token {graph_type}s?')
        plt.figtext(0.03,0.95, f'contract address: {nft_contract}')
        plt.figtext(0.03,0.93, f'most traded token: {max_traded_token}')
        plt.figtext(0.03,0.89, f'total transactions in graph: {total_transactions}')
        plt.figtext(0.51, 0.03, f'greatest number of {graph_type}s = {max(bin_dict)} | holds {bin_dict[max(bin_dict)]} tokens')
        plt.figtext(0.1, 0.03, f'number of token {graph_type}s singular: {bin_dict[1]} out of {sum(bin_dict.values())} tokens in plot | {(bin_dict[1] / sum(bin_dict.values()))*100 : 2.2f}%')
        if (total_tokens != 0):
            plt.figtext(0.03,0.91, f'average transactions per token (out of all token in existence): {(total_transactions / total_tokens)}')
            plt.figtext(0.1, 0.01, f'token {graph_type}s zero, as a percentage of all tokens ({total_tokens})  ever created: {((total_tokens - sum(bin_dict.values())) / total_tokens )*100 : 2.2f}%')
            plt.figtext(0.51, 0.01, f'singular {graph_type}s as a percentage of all tokens ever created: {(bin_dict[1] / total_tokens )*100 : 2.2f}%')
        fig.savefig(output_plot_file)

    return trans_dict


def get_total_tokens(nft_address: str):
    '''
    get total amount of tokens in nft contract from etherscan.io
    '''
    url = f'{BASE_URL}{nft_address}'
    req = ul.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    client = ul.urlopen(req)
    htmldata = client.read()

    soup = BeautifulSoup(htmldata, 'html.parser')

    script = soup.find('meta', property='og:description')
    string = script['content']
    string_start = string.find('total supply ') + len('total supply ')
    string_end = string[string_start:].find(' ')
    string = string[string_start:string_start+string_end]
    string_num = ''.join(re.findall("\d+", string))
    try:
        total_tokens = int(string_num)
        return total_tokens
    except:
        return 0

@timeit
def plot_opensea_data(df: pd.DataFrame(), number_of_graphs=10):
    '''
    wrapper function for opensea csv, plots graphs of select nft contracts by calling func_call_bin_histogram()
    '''
    nft_list = df['nft_address'].squeeze().unique()
    unique_dict = defaultdict(int)
    for nft in nft_list:
        unique_dict[nft] = len(df[df['nft_address'] == nft])
    nft_list = sorted(nft_list, key = lambda ele: unique_dict[ele], reverse=True)

    # currently skips opensea storefront, remove 1 so not to skip
    for i in range(0,number_of_graphs):
        nft_address = nft_list[i]
        nft_df = df[df['nft_address'] == nft_address]
        nft_df.to_csv('data/tmp_file.csv')
        func_call_bin_histogram(nft_address, 'data/clean_opensea.csv', f'{plots_folder}/{nft_address}_bin', 'atomicMatch_', plot=True)


@timeit
def get_wallet_cycles(filename: str) -> dict:
    cycle_dict = defaultdict(lambda: defaultdict(list))
    lines = None
    i = 0
    j = 0
    for chunk in pd.read_csv(filename, chunksize=100000, low_memory=False, dtype=str):
        for idx, row in chunk.iterrows():
            i += 1
            for token in ast.literal_eval(row['token_id']):
                cycle_dict[row['seller']][f'{row["nft_address"]}_{token}'] += [row["hash"]]
                if len(cycle_dict[row['seller']][f'{row["nft_address"]}_{token}']) > 1:
                    j += 1

    with open('data/dirty_wallets.txt', mode='w') as f:
        f.write('\n')
        f.write('Cycles ||                Wallet                      ->                 NFT address                || Token ID || Transaction Hashes\n')
        for from_hash, inner_dict in cycle_dict.items():
            for nft_token, hash_list in inner_dict.items():
                if len(hash_list) > 1:
                    nft_address = nft_token.split('_')[0]
                    token_ID = nft_token.split('_')[1]
                    f.write(f'{len(hash_list)}      || {from_hash} -> {nft_address} || {token_ID} || {hash_list}\n')
                    # number_of_dirty_wallets += 1
    number_of_dirty_wallets = cycle_dict.keys()
    with open('data/dirty_wallets.txt', mode='r') as f:
        lines = f.readlines()
        lines[0] = f'Total dirty wallets: {len(number_of_dirty_wallets)}\n'

    with open('data/dirty_wallets.txt',mode='w') as f:
        f.writelines(lines)

    return cycle_dict

    
        
    


@timeit
def opensea_meta_data(df: pd.DataFrame(), input_file='data/clean_opensea.py',output_file='/data/meta_data.txt'):
    '''
    used on a clean opensea csv file, prints out meta data about the opensea file
    input1: recieve dataframe of clean_opensea.csv
    input2: name of meta_data file
    '''
    top_num = 30

    with open(output_file,mode='w') as f:
        f.write(f'blocks: {df["block_number"].iloc[0]} ({datetime.datetime.fromtimestamp(df["block_timestamp"].iloc[0]).strftime("date %d-%m-%Y | time %H:%M:%S")}) -> {df["block_number"].iloc[-1]} ({datetime.datetime.fromtimestamp(df["block_timestamp"].iloc[-1]).strftime("date %d-%m-%Y | time %H:%M:%S")})\n')
        f.write(f'total transactions: {len(df.index)}\n')
        f.write(f"number of unique nft contracts: {df['nft_address'].squeeze().nunique()}\n")

        all_wallets = pd.Series({}, dtype=object)
        all_wallets = all_wallets.append(df['buyer'].squeeze())
        all_wallets = all_wallets.append(df['seller'].squeeze())

        f.write(f"number of unique wallets: {all_wallets.squeeze().nunique()}\n")


        unique_list = df['nft_address'].squeeze().unique()
        unique_dict = defaultdict(int)
        for nft in unique_list:
            unique_dict[nft] = len(df[df['nft_address'] == nft])

        f.write('------------------------------------------------------------------------------------------------------------\n')
        f.write(f'nft contract rankings:\n')
        i = 0
        for i in range(top_num):
            max_key = max(unique_dict, key=unique_dict.get)
            f.write(f'top {i+1} transactions: {unique_dict[max_key]} - {max_key}\n')
            unique_dict.pop(max_key)

        f.write('\n')
        i = 0
        for i in range(top_num):
            min_key = min(unique_dict, key=unique_dict.get)
            f.write(f'low {i+1} transactions: {unique_dict[min_key]} - {min_key}\n')
            unique_dict.pop(min_key)

        f.write('\n')

        res = 0
        for val in unique_dict.values():
            res += val
        f.write(f'average number of transactions per contract: {res / len(unique_dict)}\n')

        
        f.write('------------------------------------------------------------------------------------------------------------\n')
        total_spent = df[df["price"] != "alt_coin"]["price"].astype(float).sum()
        f.write(f'total ether spent in contracts: {total_spent}\n')
        df_by_price = df[df['price'] != 'alt_coin']
        df_by_price['price'] = pd.to_numeric(df_by_price['price'])
        df_by_price = df_by_price.sort_values(by=['price'],ascending=False)

        f.write(f'average ether spent on a nft: {(total_spent / len(df_by_price))}\n')
        f.write('\n')

        f.write(f'most expensive purcahses:\n')
        for i in range(top_num):
            f.write(f'{df_by_price["price"].iloc[i]} - {df_by_price["hash"].iloc[i]}\n')
        f.write('\n')

        f.write(f'least expensive purcahses:\n')
        for i in range(1,top_num+1):
            f.write(f'{df_by_price["price"].iloc[-i]} - {df_by_price["hash"].iloc[-i]}\n')

        f.write('------------------------------------------------------------------------------------------------------------\n')

def wrapper_complete_run(filename: str):
    get_wallet_cycles(filename)
    df = pd.read_csv(filename)
    opensea_meta_data(df,output_file='data/meta_data.txt')
    plot_opensea_data(df)
    print('**** Completed ANALYSIS element ****')


if __name__ == '__main__': 
    get_wallet_cycles('clean_opensea.csv')

    df = pd.read_csv('data/clean_opensea.csv', usecols = ['hash','block_number','block_timestamp','function_name','price','seller','buyer','nft_address','token_id'])
    opensea_meta_data(df,output_file='data/meta_data.txt')
    plot_opensea_data(df)