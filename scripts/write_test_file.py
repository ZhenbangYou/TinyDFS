import os
import sys
import argparse
import random
import string

def generate_random_data(size):
    return os.urandom(size)

def generate_readable_data(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size)).encode('utf-8')

def write_example_file(file_path, datanode_num, 
                       block_num, block_size, block_replication, 
                       version, data_type):
    # Create directory for all DataNodes
    os.makedirs('datanode_data', exist_ok=True)
    for i in range(datanode_num):
        datanode_dir = os.path.join('datanode_data', str(i + 1))
        os.makedirs(datanode_dir, exist_ok=True)


    for block_id in range(block_num):  
        # Generate data for the block based on the data_type
        if data_type == 'readable':
            block_data = generate_readable_data(block_size)
        else:
            block_data = generate_random_data(block_size)

        # Randomly select DataNodes for block replication
        datanodes = random.sample(range(datanode_num), block_replication)
        
        # Create test block files for chosen DataNodes
        for i, id in enumerate(datanodes):
            datanode_dir = os.path.join('datanode_data', str(id + 1))
            full_dir = os.path.join(datanode_dir, os.path.dirname(file_path))
            os.makedirs(full_dir, exist_ok=True)
        
            block_file_name = f"{file_path}_{block_id}_{version}"
            block_file_path = os.path.join(datanode_dir, block_file_name)
            
            # Write the block data to the file
            with open(block_file_path, 'wb') as block_file:
                block_file.write(block_data)
    
    print(f"Successfully wrote test file blocks to DataNodes for {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write test file blocks to DataNodes.')
    parser.add_argument('--file_path', type=str, help='Path to the file.')
    parser.add_argument('--datanode_num', type=int, default=5, help='Number of DataNodes.')
    parser.add_argument('--block_num', type=int, default=3, help='Number of blocks per file.')
    parser.add_argument('--block_size', type=int, default=64*1024, help='Size of each block in bytes.')
    parser.add_argument('--block_replication', type=int, default=3, help='Number of DataNodes to replicate each block.')
    parser.add_argument('--version', type=int, default=1, help='Version number of the blocks.')
    parser.add_argument('--data_type', type=str, choices=['random', 'readable'], default='readable', help='Type of data to generate.')
    args = parser.parse_args()

    write_example_file(args.file_path, 
                       args.datanode_num, 
                       args.block_num, 
                       args.block_size, 
                       args.block_replication,
                       args.version,
                       args.data_type)