from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import typing as tp
import timeit
import time
import numpy as np
import pandas as pd
import math
import sys
import os

def is_float(value):
    '''
    This method is used to check if a value can be converted to a float.
    '''
    try:
        float(value)
        return True
    except ValueError:
        return False

def vectorized_distance(X, N, type):
    '''
    This method was written as an alternative to the approach used to calculate the distances among all the points to determine the outliers. It exploits
    the numpy library, which is known for being faster with larger datasets. However, as stated below when calling this method (presented as a comment),
    the memory requested for very large datasets is too large for our machine to compute, so we had to revert back to the original approach we had thought
    of, which is the one presented in this file.
    :param X: The dataset in input
    :param N: The cardinality of the dataset in input
    :param type: The type of the values created inside of the arrays (this was introduced as the 100 thousand points dataset with float64 values requested
    too much memory)
    :return: A matrix containing the distances among each point of the dataset
    '''
    differences = np.array([X[i] for i in range(N)], dtype=type)[:, None] - np.array([X[j] for j in range(N)], dtype=type)[None, :]
    distances = np.linalg.norm(differences, axis=2)
    return distances

def exactOutliers(X_in, D, M, K):
    '''
    This method is used to implement the exact algorithm to calculate the number of (M,D)-outlier.
    D is a float and represents the maximum distance from the starting point to define an area in which we look for other points to determine if the starting point is an outlier or not.
    M and K are integers: M represents a threshold, whereas K is a subset of M.
    X_in instead represents a set of values in which we look for outliers, given in input as a list.
    The output is the number of (D,M)-outliers.
    '''
    X = X_in.copy() # This is just a precautional: in this way we are not directly handling the data in input.
    N = len(X)
    outliers = []
    # The following check is written because the dataset containing 100 thousand points requests too much memory with values stored as float 64 in our machines.
    if N < 50000:
        flt_type = "float64"
    else:
        flt_type = "float16"
    distances = np.zeros((N,N), dtype=flt_type) # We create an empty table where we will store the distances among the points.
    counter_col = np.zeros(N)
    overall_distance = np.zeros(N, dtype=flt_type)
    counter = 0

    #distances = vectorized_distance(X, N, flt_type) # By using this method we are faster in the bigger arrays. However, with inputs that are too big I can't
                                                     # use this approach, as it exploits the local space and with an input of 100000 points it is already not
                                                     # able to handle it.
    for i in range(0, N):
        # This check was introduced for the 100 thousend points dataset, as it required much time. It can be useful with large datasets as an indicator of how
        # long the program will take.
        # if i==50000:
            #print("---Arrived at half the dataset---")
        point = [X[i][0], X[i][1]]
        counter = 0 # Updating the counter
        for j in range(i+1, N):
            other_point = [X[j][0], X[j][1]]
            # We calculate the distance between each point: we need N*(N-1)/2 computations. This is because distances[i,j]=distances[j,i]
            distance = math.sqrt((point[0]-other_point[0])**2 + (point[1]-other_point[1])**2) # Euclidean distance between two points.
            # We used the math library as it is faster between two single points with respect to the numpy library
            distances[i,j] = distance
            distances[j,i] = distance
            overall_distance[i] += distances[i,j]
            if distances[i,j] <= D:
                counter_col[j] += 1
                counter_col[i] += 1
        # The following check is computed to efficiently evaluate the overall distance between a point and all the others in the dataset.
        if i+j != N-1:
            steps = i+j-(N-1)
            for k in range(steps):
                overall_distance[i] += distances[i,k]
    # After having obtained all the information I am interested in, I can create a list of outliers, in which I also save the distances that point has compared to the rest within the dataset.
    # The information regarding the distance is necessary in order to print the outliers in an increasing value of the distance.
    for i in range(len(counter_col)):
        if counter_col[i] < M:
            outliers.append([[X[i][0], X[i][1]], overall_distance[i], counter_col[i]])
    outliers = sorted(outliers, key=lambda x: x[2], reverse=False) # Sorting of the outliers
    print(f"The number of points in the dataset is: {N}")
    print(f"The number of outliers is: {len(outliers)}")
    if len(outliers) == 0:
        print("There are no outliers in the provided dataset.")
        return outliers
    if len(outliers) <= K:
        for i in range(len(outliers)):
            print(f"Point: {tuple(outliers[i][0])}")
        return outliers
    else:
        for i in range(K):
            print(f"Point: {tuple(outliers[i][0])}")
        return outliers

def cells_per_element(point,D): #(x,y)->((i,j),1) func. transforms real coord. into indexes of bottom-left corner the points is into
    c_size=D/np.sqrt(8)
    cell=np.ndarray(2,dtype=int)
    for i in range(2):
        cell[i]=int(np.floor(point[i]/c_size))

    c=(cell[0],cell[1])
    return[(c,1)]

def gather_part_pairs(pairs): #this function merges cells with the same indexes returning the indexes and the number of cells with those indexes
    ps=[]
    cell=np.ndarray(3)
    for p in pairs:

        cell= [p[0][0],p[0][1],p[1]]
        check=0
        for elem in ps:

            if (elem[0]==cell[0] and  elem[1]==cell[1]):
                elem[2]+=cell[2]
                check=1
        if check==0:
            ps.append(cell)

    return[((elem[0],elem[1]),elem[2]) for elem in ps]

def rddcounting(points,D): # from coord(x,y) to cell((i,j),n°points) in 2 rounds
    cells_and_number1=points.flatMap(lambda x: cells_per_element(x,D)) #MP1: (x,y)->((i,j),1)
    cells_and_number2=cells_and_number1.mapPartitions(gather_part_pairs)#RP1: multiple partitions with output cells in the form ((i,j),n°points)
    cells_and_number3=cells_and_number2.groupByKey() #grouping with outputs cells ((i,j),list of partiotion sums)
    cells_and_number4=cells_and_number3.mapValues(lambda vals: sum (vals))#RP2: output cell((i,j),n°points)

    return cells_and_number4

def swapValueKey(c_and_n): # swaps the key and the value->(n_points,(i,j)) so that the sorting can be done

    cell=np.ndarray(2)
    cell=c_and_n[0]

    n_points=c_and_n[1]

    return [(n_points,cell)]


def MRApproxOutliers(points,D,M,K):


    c_and_n=rddcounting(points,D)# from coord(x,y) to cell((i,j),n°points)

    #COMPUTING THE APPROX ALGORITHM
    nOfCells=c_and_n.count()
    flat_cn=c_and_n.collect()
    cells=np.ndarray((nOfCells,5))
    n=0
    #COPYING THE RDD INTO A LOCAL MATRIX
    for elem in flat_cn:
        cells[n,0:2]=[elem[0][0],elem[0][1]]
        cells[n,2]=elem[1]
        n+=1

    #COMPUTING R3 AND R7 FOR EACH POINT
    for n in range(nOfCells):
        counter3=0
        counter7=0
        i=cells[n,0]
        j=cells[n,1]


        for m in range(nOfCells):
            if (abs(cells[m,0]-i)<=1 and abs(cells[m,1]-j)<=1):
                counter3+=cells[m,2]
            if (abs(cells[m,0]-i)<=3 and abs(cells[m,1]-j)<=3):
                counter7+=cells[m,2]
        cells[n,3]=counter3
        cells[n,4]=counter7

    #DETECTING OUTLIERS AND UNCERTAIN POINTS
    outliers=0
    uncertain=0
    for n in range(cells.shape[0]):
        if cells[n,4]<=M:
            outliers+=int(cells[n,2])
        elif cells[n,3]<=M:
            uncertain+=int(cells[n,2])
    print("Number of sure outliers = ", outliers)
    print("Number of uncertain points = ", uncertain)

    #SORTING AND PRINTING FIRST K ELEMENTS IN ORDER
    sortedrdd=(c_and_n.flatMap(swapValueKey)
               .sortByKey()
               .take(K))
    for elem in sortedrdd:
        print("Cell: ", elem[1]," Size = ", elem[0])

def main():
    # COMMAND LINE CHECK
    # Checking the number of parameters passed through the command line: we need 6 parameters in total.
    assert len(sys.argv) == 6, "Usage: python G055HW1.py <file_name(with path)> <D> <M> <K> <L>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G055HW1')
    sc = SparkContext(conf=conf)

    # INPUT READING
    # After having checked that we have indeed the right number of parameters in command line,
    # we perform input reading to confirm that the values are in line with the expected ones.

    # 1. Read the value for the distance D
    D = sys.argv[2]
    assert is_float(D), "D must be a float"
    D = float(D)

    # 2. Read number for the threshold to determine whether a point is an outlier or not
    M = sys.argv[3]
    assert M.isdigit(), "M must be an integer"
    M = int(M)

    # 3. Read number of maximum outliers that are printed (if there are)
    K = sys.argv[4]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # 4. Read number of partitions
    L = sys.argv[5]
    assert L.isdigit(), "L must be an integer"
    L = int(L)

    # 5. Read the input file (with the proper path to it if necessary)
    data_path = sys.argv[1]
    assert os.path.isfile(data_path), "File or folder not found"

    print(f"Path: {data_path}, D={D}, M={M}, K={K}, L={L}")

    rawData = sc.textFile(data_path).repartition(numPartitions=L)

    # Transforming the RDD of strings into an RDD of numbers
    def parse_point(point_str):
        # I assume that the RDD in input store the values divided by commas
        return point_str.split(",")

    # Completing the transformation from RDD of strings into an RDD of points

    split_RDD = rawData.map(lambda x: x.split(','))
    inputPoints = split_RDD.map(lambda x: (float(x[0]), float(x[1])))
    inputPoints=inputPoints.repartition(numPartitions=L).cache()

    # As I cached the rawData RDD before, I can use this approach to show the total number of points.
    total_lines = rawData.count()
    # Assuming each line represents a point
    total_points = total_lines
    print(f"Total number of points: {total_points}")

    # Check on the number of points
    if total_points < 200000:
        listOfPoints = inputPoints.collect()
        # listOfPoints is a list of tuples representing points (x, y)
        start_exact = time.time()
        outliers = exactOutliers(listOfPoints, D, M, K)
        end_exact = time.time()
        print("Time taken to compute the exactOutliers method:", end_exact - start_exact, "seconds")

    start = time.time()
    outliers = MRApproxOutliers(inputPoints, D, M, K)
    end = time.time()
    print("Time taken to compute the MRApproxOutliers method:", end - start, "seconds")

if __name__ == "__main__":
    main()
