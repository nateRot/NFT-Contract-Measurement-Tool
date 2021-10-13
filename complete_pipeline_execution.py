import runner
import eth_carver
import data_analysis


START_BLOCK=12536766
END_BLOCK=13361368
STEP_SIZE=10000

contract_name = 'opensea'
contract_address = '0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b'.lower()

FILE_NAME = f'data/{contract_name}_{END_BLOCK}_{START_BLOCK}.csv'


if __name__ == '__main__':
    runner.wrapper_complete_run(START_BLOCK, END_BLOCK, STEP_SIZE, contract_address, contract_name)
    eth_carver.wrapper_complete_run(FILE_NAME, contract_address, contract_name, opensea=True)
    data_analysis.wrapper_complete_run(f'data/clean_{contract_name}.csv')
