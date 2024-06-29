import pprint

config = {}
config['s3_page_size'] = 10
config["testing_iteractions"] = None

max_files_range = 4
page_iterator = [
    {'Contents': [{'Key': f'file{((j*10)+i)+1}'}
     for i in range(config['s3_page_size'])]}
    for j in range(max_files_range)
]
pprint.pprint(page_iterator)

own_resume_from = -1
own_resume_from = 3
own_resume_from = 10
# own_resume_from = 12
# own_resume_from = 30

print("Starting files append...")
files = []
j = -1
for page in page_iterator:
    j += 1
    print(f"{j}) Current files: {len(files)}" +
          f" | Files to append: {len(page.get('Contents', []))}")
    first_file = 1
    if own_resume_from > 0:
        if own_resume_from > ((j+1)*config["s3_page_size"]):
            print(f"{j}) Skipping from {(j*config['s3_page_size'])+1}" +
                  f"-{(j+1)*config['s3_page_size']}" +
                  f" until {own_resume_from}...")
            continue
        else:
            first_file = own_resume_from - \
                ((j)*config["s3_page_size"])
            print(f"Starting from {first_file} | " +
                  f"originally: {own_resume_from} | " +
                  f"initial sequence: {(j)*config['s3_page_size']} ...")
            own_resume_from = -1
    curr_file = 1
    for obj in page.get('Contents', []):
        if curr_file < first_file:
            print(f"Skipping {curr_file} | {obj['Key']}")
            curr_file += 1
            continue
        print(f"Appending {curr_file} | {obj['Key']}")
        files.append(obj['Key'])
        first_file += 1
        curr_file += 1
    if config["testing_iteractions"] and \
       j >= config["testing_iteractions"]:
        break
print(f"Final total files to process: {len(files)}")
