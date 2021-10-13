#!/usr/bin/env python3

import os
import subprocess
import sys
import pandas as pd
import errno
from eth_carver import get_rows

# 12536766
START_BLOCK=13378263
END_BLOCK=13378264
STEP_SIZE=100

contract_name = 'opensea'
contract_address = '0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b'.lower()


def iterate_runner(START_BLOCK: int, END_BLOCK: int, STEP_SIZE: int, contract_address: str, contract_name: str):    
    # creates empty csv file with columns only
    if (not os.path.exists(f'data/{contract_name}_{END_BLOCK}_{START_BLOCK}.csv')):
        df = pd.DataFrame(columns=['hash','nonce','block_hash','block_number','transaction_index','from_address','to_address','value','gas','gas_price','input','block_timestamp','max_fee_per_gas','max_priority_fee_per_gas','transaction_type'])
        df.to_csv(f'data/{contract_name}_{END_BLOCK}_{START_BLOCK}.csv', index=False)

    # can remove 'else'' if you want to append existing file
    else:
        print('file already exists!')
        raise errno.EEXIST

    blocks_filename = 'data/blocks.csv'
    transaction_filename = 'data/transactions.csv'
    transaction_hashes_filename = 'data/trans_hashes.csv'
    receipts_filename = 'data/receipts.csv'
    log_filename = 'data/logs.csv'
    for curr_start in range(START_BLOCK, END_BLOCK, STEP_SIZE):
        curr_end = curr_start + STEP_SIZE - 1
        if curr_end > END_BLOCK:
            curr_end = END_BLOCK
        blocks_name = f'{START_BLOCK}_{END_BLOCK}'

        cmd = [
                "ethereumetl",
                "export_blocks_and_transactions",
                "--start-block",
                str(curr_start),
                "--end-block",
                str(curr_end),
                "--blocks-output",
                blocks_filename,
                "--transactions-output",
                transaction_filename,
                "--provider-uri",
                "http://127.0.01:8646"
        ]



        process = subprocess.Popen(cmd, bufsize=1, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                print(line,end='')
                sys.stdout.flush()
            process.wait()

        

        cmd2 = [
                "ethereumetl",
                "extract_csv_column",
                "--input",
                transaction_filename,
                "--column",
                "hash",
                "--output",
                transaction_hashes_filename
        ]

        process2 = subprocess.Popen(cmd2, bufsize=1, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process2.stdout:
            for line in iter(process2.stdout.readline, ''):
                print(line,end='')
                sys.stdout.flush()
            process2.wait()

        cmd3 = [
            "ethereumetl",
            "export_receipts_and_logs",
            "--transaction-hashes",
            transaction_hashes_filename,
            "--provider-uri",
            "http://127.0.01:8646",
            "--receipts-output",
            receipts_filename,
            "--logs-output",
            log_filename
        ]
        process3 = subprocess.Popen(cmd3, bufsize=1, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process3.stdout:
            for line in iter(process3.stdout.readline, ''):
                print(line,end='')
                sys.stdout.flush()
            process.wait()
        
        

        df = get_rows(contract_address, transaction_filename, receipts_filename)
        df.to_csv(f'data/{contract_name}_{END_BLOCK}_{START_BLOCK}.csv', mode='w', index=False, header=False)

        # os.remove(blocks_filename)
        # os.remove(transaction_filename)
        # os.remove(transaction_hashes_filename)
        # os.remove(receipts_filename)
        # os.remove(log_filename)

    


def wrapper_complete_run(START_BLOCK: int, END_BLOCK: int, STEP_SIZE: int, contract_address: str, contract_name: str):
    iterate_runner(START_BLOCK, END_BLOCK, STEP_SIZE, contract_address, contract_name)
    print()
    print('**** Completed RUNNER element ****')
    print()

if __name__ == '__main__':
    # df = get_rows(contract_address, 'data/transactions.csv', 'data/receipts.csv')
    # df.to_csv(f'data/{contract_name}_{END_BLOCK}_{START_BLOCK}.csv', mode='a', index=False, header=False)
    iterate_runner(START_BLOCK, END_BLOCK, STEP_SIZE, contract_address, contract_name)