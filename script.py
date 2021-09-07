#!/usr/bin/env python3
import os
import sys
import uuid
import json


def executeCommand(command):
    output = os.popen(command).read().strip()
    return output


def cleanup():
    print("Running clean up...")
    executeCommand("hdfs dfs -rm -r -f /task0*")
    executeCommand("rm mapper.py reducer.py 2> /dev/null")
    executeCommand("rm -rf output")


def setup():
    print("Setting up HDFS directories...")
    executeCommand("sudo apt install curl -y 2> /dev/null")
    executeCommand("hdfs dfs -mkdir /task0")
    executeCommand("hdfs dfs -put alice.txt /task0")
    executeCommand("mkdir output")
    createMapperAndReducer()


def createMapperAndReducer():
    print("Creating mapper and reducer scripts...")
    mapper_code = '''#!/usr/bin/env python3
import sys

for line in sys.stdin:
    words = line.lower().strip().split()
    for word in words:
        print(f"{word},1")
    '''

    reducer_code = '''#!/usr/bin/env python3
import sys

query_word = sys.argv[1]
total_count = 0
for line in sys.stdin:
    line = line.strip()
    word, count = line.rsplit(",", 1)
    count = int(count)
    if query_word == word:
        total_count += 1
print(total_count)'''

    with open("mapper.py", 'w') as mapper_file:
        mapper_file.write(mapper_code)

    with open("reducer.py", 'w') as reducer_file:
        reducer_file.write(reducer_code)

    executeCommand("chmod +x *.py")


def runHadoopJob():
    print("Running Hadoop MR job...")
    command = '''hadoop jar /home/$USER/hadoop-3.2.2/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
    -mapper "$PWD/mapper.py" \
    -reducer "$PWD/reducer.py 'alice'" \
    -input /task0/alice.txt \
    -output /task0/output-alice 2> output/message.txt'''
    executeCommand(command)


def getOutput():
    print("Verifying output...", end=" ")
    executeCommand(
        "hdfs dfs -cat /task0/output-alice/part-00000 > output/job-output.txt")

    with open("output/output.json", 'w') as json_outfile:
        output = dict()
        output["ass_id"] = "A0"
        output["team_id"] = TEAM_ID
        output["username"] = USERNAME
        output["ip_address"] = IP_ADDRESS
        output["mac_address"] = MAC_ADDRESS
        output["ram"] = RAM
        output["cpu_cores"] = CPU_CORES
        output["os"] = OS_VERSION
        output["mac_address"] = MAC_ADDRESS
        output["marks"] = 0
        output["message"] = ""

        with open("output/job-output.txt") as joboutfile:
            output["output"] = joboutfile.read().strip()
            if not output["output"]:
                print("ERROR!")
                with open("output/message.txt") as logfile:
                    lines = logfile.readlines()
                    for line in lines:
                        if "hadoop: not found" in line:
                            output["message"] = "Hadoop has NOT been installed or your configuration is incorrect"
                            break
                        elif line.startswith("Error:") or "Error" in line:
                            output["message"] = line.strip()
                            break
            else:
                print("SUCCESS")
                output["marks"] = 1
                output["message"] = "Hadoop has been successfully installed and configured"

        json.dump(output, json_outfile, indent=4)


def sendOutput():
    print("Sending result to server...")
    executeCommand(
        'curl -sS -X POST -H "Content-Type: application/json" -m 10 -d @output/output.json http://34.133.239.238:5000/assignment0')


try:
    TEAM_ID = sys.argv[1]
except IndexError:
    print("Invalid Team ID.\nPlease run command using: python3 script.pyc TEAM_ID")
    exit(0)

USERNAME = os.environ.get('USER').upper()
IP_ADDRESS = executeCommand("hostname -I | awk '{print $1}'")
RAM = round(float(executeCommand(
    "grep MemTotal /proc/meminfo | awk '{print $2 / 1024}'")))
CPU_CORES = int(executeCommand("nproc"))
OS_VERSION = executeCommand("hostnamectl | grep Operating| cut -c21-")
MAC_ADDRESS = (':'.join(['{:02x}'.format(
    (uuid.getnode() >> ele) & 0xff) for ele in range(0, 8*6, 8)][::-1]))

print("Starting Hadoop Installation verification...\n")
cleanup()
setup()
runHadoopJob()
getOutput()
sendOutput()
# cleanup()
print("\nVerification concluded. Submission has been made to the portal.\nPlease check this portal for the results: http://34.133.239.238:5000/")
