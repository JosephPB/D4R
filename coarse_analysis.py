import numpy as np
import pandas as pd
import progressbar

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

    prefixes = np.asarray(prefixes)
    prefixes = prefixes.astype(int)
    
    unique, counts = np.unique(prefixes, return_counts=True)
    print ('there are {} calls made by callers with prefix {} in this dataset'.format(counts[1], prefix))
    
    #identify callerIDs with the correct prefix
    to_analyse = np.unique(callerID[prefixes == prefix])

    #apply time constraints
    h1 = callerID[timestamp.hour >= 18]
    h2 = callerID[timestamp.hour <= 7]
    h = np.append(h1,h2)
    overlap = np.intersect1d(h,to_analyse)
    print ('there are {} callers who match the time constrains with prefix {}'.format(len(overlap), prefix))

    #create dictionary
    evening_location = {}
    for i in overlap:
        evening_location[str(i)] = {
            'cities':[],
            'dates':[]
        }

    #bottleneck - could parallelise better
    print ('writing information to dictionary:')
    bar = progressbar.ProgressBar(maxval=len(overlap), widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    
    counter = 0
    for i in overlap:
        location =  np.where(callerID == i)
        cities = cityID[location[0]]
        evening_location[i]['cities'] = cities
        dates = date[location[0]]
        evening_location[i]['dates'] = dates
        bar.update(counter)
        counter +=1
    bar.finish()

        
    return evening_location



def mobility(dictionary, low_threshold = 2, medium_threshold = 5):

    #find the refugees who spend their time in more that one city in the evening over the month
    mobile = []
    for i in dictionary:
        unique, counts = np.unique(dictionary[i]['cities'], return_counts=True)
        if len(unique) > 1:
            mobile.append(i)
            
    print ('there are {} callers in this dataset who moved cities in this dataset'.format(len(mobile)))

    #assign an entry to each callerID which gives the cities 'moved' to
    for i in mobile:
        moved = []
        moved.append(dictionary[i]['cities'][0])
        counter = 0
        for j in range(1,len(dictionary[i]['cities'])):
            if dictionary[i]['cities'][j] == dictionary[i]['cities'][j-1]:
                counter += 1
            else:
                counter = 0
            if counter == 4 and dictionary[i]['cities'][j] != moved[-1]:
                moved.append(dictionary[i]['cities'][j])
        dictionary[i]['moved'] = moved

    #class callers as low, medium and high mobility
    low_mobility = []
    medium_mobility = []
    high_mobility = []
    for i in mobile:
        if len(dictionary[i]['moved']) <= low_threshold:
            low_mobility.append(i)
        elif low_threshold < len(dictionary[i]['moved']) <= medium_threshold:
            medium_mobility.append(i)
        else:
            high_mobility.append(i)

    print ('there are {} callers classed as low mobility in this dataset'.format(len(low_mobility)))
    print ('there are {} callers classed as medium mobility in this dataset'.format(len(medium_mobility)))
    print ('there are {} callers classed as high mobility in this dataset'.format(len(high_mobility)))

    return low_mobility, medium_mobility, high_mobility, dictionary

if __name__ == '__main__':

    dictionary_main = {}
    
    with open('dataset_3.txt', 'r') as dataset_3:
        for line in dataset_3:
            data = pd.read_csv(line[:-1])

            dictionary_sub = evening_locations(data,2)

            for i in dictionary_sub:
                if i in dictionary_main:
                    dictionary_main[i]['cities'] = np.append(dictionary_main[i]['cities'], dictionary_sub['cities'])
                    dictionary_main[i]['dates'] = np.append(dictionary_main[i]['cities'], dictionary_sub['dates'])
                else:
                    dictionary_main[i] = {
                        'cities': dictionary_sub[i]['cities'],
                        'dates': dictionary_sub[i]['dates']
                    }
    print (len(dictionary_main))

            
