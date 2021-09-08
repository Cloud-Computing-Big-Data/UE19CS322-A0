import json
import pandas as pd
import requests

def saveSubmissionDetails(team_details, filename):
    writer = pd.ExcelWriter(f'{filename}.xlsx', engine='xlsxwriter')
    team_details_df = pd.DataFrame.from_dict(team_details, orient='index')
    all_srn = list()
    for row in team_details_df.iterrows():
        row = dict(row[1])
        srns = [row[0], row[1], row[2], row[3]]
        srns = [srn for srn in srns if srn != None]
        srns = '\n'.join(srns)
        all_srn.append(srns)

    team_details_df.drop(columns=[0, 1, 2, 3], inplace=True)
    team_details_df["SRN"] = all_srn
    team_details_df.to_excel(writer, sheet_name='Sheet 1')

    workbook = writer.book
    worksheet = writer.sheets['Sheet 1']
    format = workbook.add_format({'text_wrap': True})
    worksheet.set_column('A:C', None, format)
    writer.save()


response = requests.post("http://34.133.239.238:5000/getresults")
a0_submission = response.json()

with open("data/team_details.json") as json_file:
    team_details = json.loads(json_file.read())

submitted = dict()
not_submitted = dict()

submitted_ctr = 0
not_submitted_ctr = 0

for team_id in team_details:
    for srn in team_details[team_id]["srn"]:
        if team_id in a0_submission:
            if srn in a0_submission[team_id]:
                if "Hadoop has been configured correctly" in a0_submission[team_id][srn]:
                    if team_id not in submitted:
                        submitted[team_id] = list()
                    submitted[team_id].append(srn)
                    submitted_ctr += 1
                else:
                    if team_id not in not_submitted:
                        not_submitted[team_id] = list()
                    not_submitted[team_id].append(srn)
                    not_submitted_ctr += 1
            else:
                if team_id not in not_submitted:
                    not_submitted[team_id] = list()
                not_submitted[team_id].append(srn)
                not_submitted_ctr += 1
        else:
            if team_id not in not_submitted:
                not_submitted[team_id] = list()
            not_submitted[team_id].extend(team_details[team_id]["srn"])
            not_submitted_ctr += len(team_details[team_id]["srn"])
            break

# json_object_submitted = json.dumps(submitted, indent=4)
# with open("A0-Submissions.json", 'w') as json_file:
#     json_file.write(json_object_submitted)
# 
# json_object_not_submitted = json.dumps(not_submitted, indent=4)
# with open("A0-Missing.json", 'w') as json_file:
#     json_file.write(json_object_not_submitted)

print(f"SUBMITTED: {submitted_ctr}\nNOT SUBMITTED: {not_submitted_ctr}")
saveSubmissionDetails(submitted, "data/A0-Submissions")
saveSubmissionDetails(not_submitted, "data/A0-Missing")
