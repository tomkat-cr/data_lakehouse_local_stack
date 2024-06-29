batch_size = 6
len_file_list = 50
print(f"Batch size: {batch_size}")
print(f"Total files to process: {len_file_list}")
file_list = [f"file_{i+1}.json" for i in range(0, len_file_list)]
j = 0
for i in range(0, len_file_list, batch_size):
    j += 1
    up_to = i+batch_size
    # if up_to > len_file_list:
    #     up_to = len_file_list + 1
    #     print(f"Last batch sent. {i+batch_size} adjusted to: {up_to}")
    batch_files = file_list[i:up_to]
    print(f"{j}) From: {i+1} | To: {up_to} | Files: {batch_files}")
