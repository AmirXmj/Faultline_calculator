import pandas as pd
import numpy as np
from tqdm import tqdm as tqdm
import math
import multiprocessing
from multiprocessing import Pool
import swifter
cores=math.floor(multiprocessing.cpu_count()*3/4)
import more_itertools as mit
from sklearn.metrics.pairwise import cosine_similarity
import csv

def item_split_cal(set,mean_0,total_ss_0):
    
    [k1,k2]= set

    fau=[k1,((( df_CO.loc[ df_CO.iloc[:,0].isin(k1)].iloc[:,1:].mean()- mean_0).apply(lambda x:x**2)*len(k1)).sum()+(( df_CO.loc[ df_CO.iloc[:,0].isin(k2)].iloc[:,1:].mean()- mean_0).apply(lambda x:x**2)*len(k2)).sum())/total_ss_0]
        
    return [k1,fau]

def fau_calc(df):
    split_obj=mit.set_partitions(list(df['Unnamed: 0']), 2)
    chunks=mit.chunked(split_obj,1000000)
    print('mean done!  ')
    mean_0=df.iloc[:,1:].mean()
    total_ss_0=df.iloc[:,1:].swifter.set_dask_scheduler('processes').set_npartitions(cores).apply(lambda x:(x- mean_0)**2,axis= 1).sum(axis=1).sum()
    print('SS done! ',total_ss_0)

    fau_dict=[]
    for chunk in tqdm(chunks):
        sets=list(chunk)
        print('the number of splits :', len(sets))
        with Pool(cores) as p:
            fau_dict_part=[p.apply(item_split_cal, args=(set, mean_0, total_ss_0)) for set in tqdm(sets)]
            p.terminate()
            p.join()
        fau_dict=fau_dict+fau_dict_part
        break

    return fau_dict

df_CO = pd.read_csv("CO_IPV.csv",sep=',')
for i in  df_CO['Unnamed: 0']:
    if type(i) is not str:
         df_CO['Unnamed: 0'].at[ df_CO['Unnamed: 0']==i]=str(i)
        
df_CO_cosins_17=1-pd.DataFrame(cosine_similarity( df_CO.iloc[:,1:]))
df_CO_cosins_17.index =  df_CO['Unnamed: 0']
df_CO_cosins_17.columns =  df_CO['Unnamed: 0']
df_CO_cosins_17.to_csv('distances.csv',index=False)

df_CO.index = np.arange(1, len( df_CO['Unnamed: 0']) + 1)

mean_0= df_CO.iloc[:,1:].mean()
print('mean done!  ')
total_ss_0= df_CO.iloc[:,1:].swifter.set_dask_scheduler('processes').set_npartitions(cores).apply(lambda x:(x- mean_0)**2,axis= 1).sum(axis=1).sum()
print('SS done! ',total_ss_0)
split_obj=mit.set_partitions(list( df_CO['Unnamed: 0'])[:4], 2)
chunks=mit.chunked(split_obj,2)
def call_an(chunk):
    split=list(chunk)
    set=[]
    for i in tqdm(split):
            if len(i[0])<6:
                set.append(i)
    return set
    
sets=[]
with Pool(cores) as p:
        sets.append(p.map(call_an,chunks))
        p.terminate()
        p.join()
lst=[]
for i in tqdm(sets[0]):
    for j in i:
            if len(j)>0:
                lst.append([j,mean_0,total_ss_0])
                
def item_split_cal(set,mean_0,total_ss_0):
    
    [k1,k2]= set

    fau=[k1,((( df_CO.loc[ df_CO.iloc[:,0].isin(k1)].iloc[:,1:].mean()- mean_0).apply(lambda x:x**2)*len(k1)).sum()+(( df_CO.loc[ df_CO.iloc[:,0].isin(k2)].iloc[:,1:].mean()- mean_0).apply(lambda x:x**2)*len(k2)).sum())/total_ss_0]
        
    return [k1,fau]

def fau_calc(df):
    split_obj=mit.set_partitions(list(df['Unnamed: 0']), 2)
    chunks=mit.chunked(split_obj,1000000)
    print('mean done!  ')
    mean_0=df.iloc[:,1:].mean()
    total_ss_0=df.iloc[:,1:].swifter.set_dask_scheduler('processes').set_npartitions(cores).apply(lambda x:(x- mean_0)**2,axis= 1).sum(axis=1).sum()
    print('SS done! ',total_ss_0)

    fau_dict=[]
    for chunk in tqdm(chunks):
        sets=list(chunk)
        print('the number of splits :', len(sets))
        with Pool(cores) as p:
            fau_dict_part=[p.apply(item_split_cal, args=(set, mean_0, total_ss_0)) for set in tqdm(sets)]
            p.terminate()
            p.join()
        fau_dict=fau_dict+fau_dict_part
        break

    return fau_dict

faus=fau_calc(df_CO)
a_file = open("CO_fau.csv", "w")
writer = csv.writer(a_file)
for i in faus:
    writer.writerow([i[0],';',i[1]])
a_file.close()