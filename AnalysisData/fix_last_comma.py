import sys
import os
if len(sys.argv) < 2:
    print("ERROR: specify the filename to be fixed")
    sys.exit(-1)
file_name = sys.argv[1]
temp_file_name = F"{file_name}+.tmp"
os.system(F"cp {file_name} {temp_file_name}")
with open (temp_file_name,'r') as in_stream:
    with open(file_name, 'w') as out_stream:
        for line in in_stream:
            if line.endswith(",\n"):
                last_comma_position = line.rfind(",")
                line= line[:last_comma_position] + "\n"
            out_stream.write(line)

os.system(F"rm {temp_file_name}")