import pandas as pd
from multiprocessing import Pool
from collections import defaultdict


def map_function(data):
    passenger_count = defaultdict(int)
    for row in data:
        passenger_count[row] += 1
    return passenger_count

def shuffle_function(map):
    shuffle = defaultdict(list)
    for d in map:
        for passenger_id, count in d.items():
            shuffle[passenger_id].append(count)
    return shuffle

def reduce_function(shuffle):
    result = defaultdict(int)
    for passenger_id, count_list in shuffle.items():
        result[passenger_id] = sum(count_list)
    return result

def main():
    # Read data
    df =  pd.read_csv('data\\AComp_Passenger_data_no_error_DateTime.csv',header=None)
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    passenger = df.iloc[:,0].tolist()

    # Split data into four parts
    subtask = [passenger[i::4] for i in range(4)]

    # Create a multiprocessing Pool
    with Pool(processes=4) as pool:
        # Map phase: use pool.map to apply the map_function to the data in parallel
        map_results = pool.map(map_function, subtask)

    shuffle_results = shuffle_function(map_results)

    # Reduce phase: merge results
    result = reduce_function(shuffle_results)

    # Find the passenger with the highest number of flights
    max_flights_passenger = max(result, key=result.get)

    print("max_flights_passenger:",max_flights_passenger)
    print("max_flights:",result[max_flights_passenger])


if __name__ == '__main__':
    main()