import numpy as np
import pandas as pd

def evening_locations(data, prefix, lower_bound = 7, upper_bound = 18):

    #load in data
    data = np.asarray(data)
    length, width = data.shape
    print ('there are {} calls recorded in this dataset'.format(length))
    
    #extract callerIDs
    callerID = data[:,0]
    callerID = callerID.astype(str)
    
    #extract timestamp
    timestamp = pd.to_datetime(data[0:length,1], format="%d-%m-%Y %H:%M")
    date = np.asarray(timestamp.date)

    #extract districtIDs
    districtID = data[:,2]
        
    #extract cityIDs
    cityID = data[:,3]

    #extract prefixes
    prefixes = []
    for i in callerID:
        prefixes.append(i[0])

    prefixes = np.asarray(prefix)
    prefixes = prefixes.astype(int)
    
    unique, counts = np.unique(prefixes, return_counts=True)
    print ('there are {} refugee calls in this dataset'.format(counts[1]))
    
    #identify callerIDs with the correct prefix
    to_analyse = np.unique(callerID[prefixes == prefix])
    print (len(to_analyse))

    #apply time constraints
    h1 = callerID[timestamp.hour >= 18]
    h2 = callerID[timestamp.hour <= 7]
    h = np.append(h1,h2)
    overlap = np.intersect1d(h,to_analyse)
    print (len(overlap))

    #create dictionary
    evening_location = {}
    for i in overlap:
        evening_location[str(i)] = {
            'cities':[],
            'dates':[]
        }

    #bottleneck - could parallelise better
    counter = 0
    for i in overlap:
        location =  np.where(callerID == i)
        cities = cityID[location[0]]
        evening_location[i]['cities'] = cities
        dates = date[location[0]]
        evening_location[i]['dates'] = dates
        counter +=1
        if counter%100 == 0:
            print (counter)

    return evening_location

data = pd.read_csv('./Dataset 3/Dataset 3_201701_In.csv')

dic = evening_locations(data,2)

mobile = [] 
for i in dic:
    unique, counts = np.unique(dic[i]['cities'], return_counts=True)
    if len(unique) > 1:
        mobile.append(i)

print ('there are {} callers out ot 50,000 refugees who moved cities'.format(len(mobile)))
