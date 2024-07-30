import argparse
import concurrent.futures
import configparser
import json
import logging
import multiprocessing
import random
import string
import time
import uuid
from datetime import datetime
import sys
import os

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read configuration from default.ini
config = configparser.ConfigParser()
config.read('default.ini')

# Use the log file from the configuration
log_file = config['DEFAULT']['log_file']
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)


def parse_args():

    parser = argparse.ArgumentParser(
        description='A console utility to generate test data in JSON format based on a specified schema.')
    parser.add_argument('--files_count', type=int, default=config['DEFAULT'].getint('files_count'),
                        help='Number of JSON files to generate (default: %(default)s)')
    parser.add_argument('--file_name', type=str, default=config['DEFAULT']['file_name'],
                        help='Base file name (default: %(default)s)')
    parser.add_argument('--file_prefix', type=str, choices=['count', 'random', 'uuid'], default=None,
                        help='Prefix for file name (default: None)')
    parser.add_argument('--data_schema', type=str, default=config['DEFAULT']['data_schema'],
                        help='Path to JSON schema file or schema in JSON format')
    parser.add_argument('--data_lines', type=int, default=config['DEFAULT'].getint('data_lines'),
                        help='Number of lines for each file (default: %(default)s)')
    parser.add_argument('--path_to_save_files', type=str, default=config['DEFAULT']['path_to_save_files'],
                        help='Path to save output files (default: %(default)s)')
    parser.add_argument('--clear_path', action='store_true',
                        help='Clear the output directory before generating new files')
    parser.add_argument('--multiprocessing', type=int, default=config['DEFAULT'].getint('multiprocessing', 1),
                        help='Number of processes to use (default: %(default)s)')
    args, unknown = parser.parse_known_args()
    for u in unknown:
        logging.warning(f'Unknown argument: {u}')
    return args

def generate_random_data(schema):
    data = {}
    for key, value in schema.items():
        dtype, dparams = value.split(':', 1)

        # Handle timestamp
        if dtype == 'timestamp':
            data[key] = datetime.now().isoformat()

        # Handle string
        elif dtype == 'str':
            if dparams == 'rand':
                data[key] = str(uuid.uuid4())
            else:
                if dparams.startswith('[') and dparams.endswith(']'):
                    new_params = dparams[1:-1].split(',')
                    if len(new_params) == 1:
                        logging.error(f'Error: {key} has no elements')
                        exit(1)
                    new_params = [x.replace("'", "") for x in new_params]
                    data[key] = random.choice(new_params)
                else:
                    data[key] = dparams

        # Handle integer
        elif dtype == 'int':
            if dparams.startswith('rand'):
                if dparams == 'rand':
                    data[key] = random.randint(0, 10000)
                else:
                    if dparams.startswith('rand(') and dparams.endswith(')'):
                        new_params = dparams[5:-1].split(',')
                        if len(new_params) != 2:
                            logging.error(f'Error: {key} has invalid parameters')
                            exit(1)
                        try:
                            a = int(new_params[0])
                            b = int(new_params[1])
                        except:
                            logging.error(f'Error: {new_params} is not an integer')
                            exit(1)
                        data[key] = random.randint(a, b)
                    else:
                        logging.error(f'Error: {dparams} is not a valid parameter')
                        exit(1)

            elif dparams == '':
                data[key] = None

            else:
                if dparams.startswith('[') and dparams.endswith(']'):
                    new_params = dparams[1:-1].split(',')
                    if len(new_params) == 0:
                        logging.error(f'Error: {key} has no elements')
                        exit(1)
                    for i in new_params:
                        try:
                            int(i)
                        except:
                            logging.error(f'Error: {i} is not an integer')
                            exit(1)
                    data[key] = random.choice(new_params)
                else:
                    try:
                        int(dparams)
                    except:
                        logging.error(f'Error: {dparams} is not an integer')
                        exit(1)
                    data[key] = dparams

    return data

def generate_data(output_file, num_records, schema):
    schema = json.loads(schema)
    with open(output_file, 'w') as f:
        data = [generate_random_data(schema) for _ in range(num_records)]
        json.dump(data, f, indent=4)


def worker(file_tasks):
    for file_name, num_records, schema in file_tasks:
        try:
            generate_data(file_name, num_records, schema)
        except Exception as e:
            logging.error(f'Error occurred: {e}')
            exit(1)



def main():
    try:
        args = parse_args()
    except Exception as e:
        logging.error(f'Argument parsing error: {e}')
        sys.exit(1)

    # Validate path_to_save_files
    if not os.path.exists(args.path_to_save_files):
        os.makedirs(args.path_to_save_files)
    elif not os.path.isdir(args.path_to_save_files):
        logging.error(f'Error: {args.path_to_save_files} is not a directory')
        exit(1)

    if args.clear_path:
        for filename in os.listdir(args.path_to_save_files):
            if filename.startswith(args.file_name):
                os.remove(os.path.join(args.path_to_save_files, filename))

    # Load schema
    try:
        if os.path.isfile(args.data_schema):
            with open(args.data_schema, 'r') as f:
                schema = f.read()
        else:
            schema = args.data_schema
    except Exception as e:
        logging.error(f'Error reading schema: {e}')
        exit(1)

    if args.files_count < 0:
        logging.error('Error: files_count cannot be less than 0')
        exit(1)

    file_prefix = args.file_prefix

    if args.files_count == 1:
        file_prefix = None
        data = [generate_random_data(json.loads(schema)) for _ in range(args.data_lines)]
        with open(os.path.join(args.path_to_save_files, f'{args.file_name}.json'), 'w') as f:
            json.dump(data, f, indent=4)

    if args.files_count == 0:
        data = [generate_random_data(json.loads(schema)) for _ in range(args.data_lines)]
        print(json.dumps(data, indent=4))

    else:
        if args.multiprocessing < 1:
            logging.error('Error: multiprocessing cannot be less than 1')
            exit(1)

        # Adjust the number of processes if necessary
        num_processes = min(args.multiprocessing, os.cpu_count())

        with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
            if file_prefix == 'count':
                logging.info(f'Generating {args.files_count} files with prefix {args.file_name}')
                futures = [
                    executor.submit(worker, [(os.path.join(args.path_to_save_files, f'{i}_{args.file_name}.json'),
                                              args.data_lines, schema)]) for i in range(args.files_count)
                ]

            elif file_prefix == 'random':
                logging.info(f'Generating {args.files_count} files with random prefix')
                futures = [
                    executor.submit(worker, [
                        (os.path.join(args.path_to_save_files, f'{str(uuid.uuid4())}_{args.file_name}.json'),
                         args.data_lines, schema)]) for i in range(args.files_count)
                ]
            elif file_prefix == 'uuid':
                logging.info(f'Generating {args.files_count} files with uuid prefix')
                futures = [
                    executor.submit(worker, [(os.path.join(args.path_to_save_files, f'{str(uuid.uuid4())}.json'),
                                              args.data_lines, schema)]) for i in range(args.files_count)
                ]
            else:
                logging.info(f'Generating {args.files_count} files')
                futures = [
                    executor.submit(worker, [(os.path.join(args.path_to_save_files, f'{i}_{args.file_name}.json'),
                                              args.data_lines, schema)]) for i in range(args.files_count)
                ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f'Error occurred: {e}')
                    exit(1)

    logging.info('Data generation completed successfully')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        exit(1)

