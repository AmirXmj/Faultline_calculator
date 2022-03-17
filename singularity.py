import pandas as pd
from tqdm import tqdm as tqdm
import requests
import json
import csv

df_CO = pd.read_csv("to_publish.csv",sep=',')
df_CO=df_CO[:17]

intCodeLst=[]
singularities_10={}
singularities_2={}
singularities_5={}

for i in df_CO['Geographic Regions']:
    singularities_2[i]=[]
for i in df_CO['Geographic Regions']:
    singularities_5[i]=[]
for i in df_CO['Geographic Regions']:
    singularities_10[i]=[]

for i in tqdm(df_CO.iloc[:,1:].columns):
    if df_CO[i].max()>df_CO[df_CO[i]!=df_CO[i].max()][i].max()*10:
        singularities_10[df_CO[df_CO[i]==df_CO[i].max()]['Geographic Regions'].values[0]].append([i,df_CO[i].max()])
        if i not in intCodeLst:
            intCodeLst.append(i)
    if df_CO[i].max()>df_CO[df_CO[i]!=df_CO[i].max()][i].max()*2:
        singularities_2[df_CO[df_CO[i]==df_CO[i].max()]['Geographic Regions'].values[0]].append([i,df_CO[i].max()])
        if i not in intCodeLst:
            intCodeLst.append(i)
    if df_CO[i].max()>df_CO[df_CO[i]!=df_CO[i].max()][i].max()*2:
        singularities_5[df_CO[df_CO[i]==df_CO[i].max()]['Geographic Regions'].values[0]].append([i,df_CO[i].max()])
        if i not in intCodeLst:
            intCodeLst.append(i)


int_dict={}
no_name=[]
for i in tqdm(intCodeLst):
    try:
        link='https://graph.facebook.com/v5.0/search?access_token=<TOKEN>&locale=en_US&interest_fbid_list=["'+str(i)+'"]&type=adinterestvalid'
        r=requests.get(link)
        response = json.loads(r.text)
        int_dict[i]=response["data"][0]['name']
    except:
        print(i)
        no_name.append(i)
        

nm_sin_10={}
for key in singularities_10:
    sing_lst=[]
    for i in singularities_10[key]:
        sing_lst.append([int_dict[i[0]],i[1]])
    nm_sin_10[key]=sing_lst
nm_sin_2={}
for key in singularities_2:
    sing_lst=[]
    for i in singularities_2[key]:
        sing_lst.append([int_dict[i[0]],i[1]])
    nm_sin_2[key]=sing_lst
nm_sin_5={}
for key in singularities_5:
    sing_lst=[]
    for i in singularities_5[key]:
        sing_lst.append([int_dict[i[0]],i[1]])
    nm_sin_5[key]=sing_lst

with open('int_dict.txt', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in int_dict.items():
            writer.writerow([key, value])

with open('nm_sin_10.txt', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in nm_sin_10.items():
            writer.writerow([key, value])
with open('nm_sin_2.txt', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in nm_sin_2.items():
            writer.writerow([key, value])
import csv
with open('nm_sin_5.txt', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in nm_sin_5.items():
            writer.writerow([key, value])


