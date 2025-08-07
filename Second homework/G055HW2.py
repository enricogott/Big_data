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


def isInt(val):
    """
    Manual check to verify if the input is an integer
    :param val: Value to be checked
    :return: The value as an integer if possible, 'False' otherwise, which triggers the error response
    """
    try:
        num = int(val)
        return num
    except ValueError:
        return False


def distance(p1, p2):
    """
    This method is used to implement the Euclidean distance between two points in the Euclidean space (R2).
    :param p1: First point to be considered in the RDD, type float.
    :param p2: Second point to be considered in the RDD, type float.
    :return: The Euclidean distance between point 1 and point 2, type float.
    """
    return math.sqrt(math.pow(p1[0] - p2[0],2) + math.pow(p1[1] - p2[1],2))


def point2center(point, C):
    """
    Calculate the distance between a point and the nearest center in a given set of centers.
    :param point: RDD of points.
    :param C: A list of centers, where each center is represented as a tuple (x, y).
    :return: A list containing the distance between the point and the nearest center.
    """
    dist = distance(point, C[0])

    for center in C[1:]:
        new_dist = distance(point, center)
        if new_dist < dist:
            dist = new_dist

    return [(dist)]


def mapping_points(point):  
    """
    Mapping criteria for MP1, (x,y)->((x,y),1)
    :param point: point is an element of the RDD of points
    :return: the point coords as the key, an arbitrary constant as the value
    """
    return [(point[0], point[1])]


def SequentialFFT(points, k):
    """
    This method implement the Furthest-First Traversal algorithm seen in class.
    Used for the implementation of the first round of the Map Reduce algorithm.
    :param points: A partition of the RDD containing points. It is expected an RDD with points in the
                   Euclidean space.
    :param k: Number of centers for the k-center clustering approach, type int.
    :return: A set S containing the k centers found with the FFT.
    """

    C = []
    max_d = 0
    furthest_point = None
    discarded_distance = None
    distances = []
    C.append(points[0])  # We take an arbitrary point (in this case the first) to be the first center
    del points[0]  # I remove the first point
    for i in range(k-1):
        max_d = 0
        furthest_point = None
        discarded_distance = None
        for count, point in enumerate(points):  # Initialization of the centers: obtaining the second center given the first
            new_distance = distance(point, C[i])
            if i == 0:  # Used for initialization
                distances.append(new_distance)  # Initializing the distances for each point
            else:  # Used for updating (This check could be exploited with a min function)
                if distances[count] > new_distance:
                    distances[count] = new_distance
            if distances[count]> max_d:
                max_d = distances[count]
                furthest_point = point
                discarded_distance = count
        C.append(furthest_point)
        # points.remove(furthest_point)  # Maybe needs to be commented: removing the point takes time and the pros of doing so could be very little
        del points[discarded_distance]
        del distances[discarded_distance]  # I take out also the distance linked to the removed point
    return C



def MRFFT(points, k, sc):  # from coord(x,y) to cell((i,j),n°points) in 2 rounds
    """
    Perform MRFFT algorithm to compute the maximum radius of clusters.
    :param points:RDD of points
    :param k: The n° of centers we want as the output of SequentialFFT
    :param sc: Spark context to be used in the broadcast operation
    :return: The max Radius of the clusters
    """
    
    #Round 1
    start_r1 = time.time()
    centers_per_partition = points.mapPartitions(lambda x:SequentialFFT(list(x),k)).cache()  
    
    centers_collected = centers_per_partition.collect()
    end_r1 = time.time()
    print("Running time of MRFFT Round 1 =", int((end_r1 - start_r1) * 1000), "ms")
    
    #Round 2
    start_r2 = time.time()
    
    new_centers = SequentialFFT(centers_collected, k)
    end_r2 = time.time()
    print("Running time of MRFFT Round 2 =", int((end_r2 - start_r2) * 1000), "ms")
    
    
    #Round 3
    start_r3 = time.time()
    C = sc.broadcast(new_centers)  # Creating the broadcast variable for the centers
    clust_dist = points.flatMap(lambda x: point2center(x, C.value)).persist()  
    radius = clust_dist.reduce(max)
    end_r3 = time.time()
    print("Running time of MRFFT Round 3 =", int((end_r3 - start_r3) * 1000), "ms")
    print("Radius =", radius)

    return radius


def cells_per_element(point, D):  
    """
    Convert real coordinates into indexes of the bottom-left corner of the cell the point belongs to.
    :param point: The coordinates of the point of the RDD as a tuple (x, y).
    :param D: The size of the domain.
    :return: A list containing a tuple representing the cell index and the count of points in that cell.
    """
    c_size = D/np.sqrt(8)
    cell = np.ndarray(2,dtype=int)
    for i in range(2):
        cell[i] = int(np.floor(point[i]/c_size))

    c=(cell[0],cell[1])
    return[(c,1)]


def gather_part_pairs(pairs): 
    """
    Merge cells with the same indexes and return the indexes along with the number of cells with those indexes.
    :param pairs: A list of tuples where each tuple contains a cell index and the count of cells with that index.
    :return: A list of tuples where each tuple contains a merged cell index and the total count of cells with that index.
    """
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


def rddcounting(points, D):  
    """
    Count the number of points per cell using RDDs, transforming coordinates into cell indexes.
    :param points: RDD of points.
    :param D: The size of the domain.
    :return: RDD containing cell indexes and the total count of points in each cell.
    """
    cells_and_number1=points.flatMap(lambda x: cells_per_element(x,D))  #MP1: (x,y)->((i,j),1)
    cells_and_number2=cells_and_number1.mapPartitions(gather_part_pairs)  #RP1: multiple partitions with output cells in the form ((i,j),n°points)
    cells_and_number3=cells_and_number2.groupByKey()  #grouping with outputs cells ((i,j),list of partiotion sums)
    cells_and_number4=cells_and_number3.mapValues(lambda vals: sum (vals))  #RP2: output cell((i,j),n°points)

    return cells_and_number4


def MRApproxOutliers(points, D, M):
    """
    Detect sure and uncertain outliers using the MRApprox algorithm.
    :param points: RDD of points.
    :param D: The size of the domain.
    :param M: The threshold value.
    :return: None
    """
    c_and_n = rddcounting(points, D)  # from coord(x,y) to cell((i,j),n°points)

    # COMPUTING THE APPROX ALGORITHM
    nOfCells = c_and_n.count()
    flat_cn = c_and_n.collect()
    cells = np.ndarray((nOfCells, 5))
    n = 0
    # COPYING THE RDD INTO A LOCAL MATRIX
    for elem in flat_cn:
        cells[n, 0:2] = [elem[0][0], elem[0][1]]
        cells[n, 2] = elem[1]
        n += 1

    # COMPUTING R3 AND R7 FOR EACH POINT
    for n in range(nOfCells):
        counter3 = 0
        counter7 = 0
        i = cells[n, 0]
        j = cells[n, 1]

        for m in range(nOfCells):
            if (abs(cells[m, 0] - i) <= 1 and abs(cells[m, 1] - j) <= 1):
                counter3 += cells[m, 2]
            if (abs(cells[m, 0] - i) <= 3 and abs(cells[m, 1] - j) <= 3):
                counter7 += cells[m, 2]
        cells[n, 3] = counter3
        cells[n, 4] = counter7

    # DETECTING OUTLIERS AND UNCERTAIN POINTS
    outliers = 0
    uncertain = 0
    for n in range(cells.shape[0]):
        if cells[n, 4] <= M:
            outliers += int(cells[n, 2])
        elif cells[n, 3] <= M:
            uncertain += int(cells[n, 2])
    print("Number of sure outliers =", outliers)
    print("Number of uncertain points =", uncertain)


def main():
    assert len(sys.argv) == 5

    conf = SparkConf().setAppName('G055HW2')
    conf.set("spark.locality.wait", "0s")
    sc = SparkContext(conf=conf)

    M = sys.argv[2]
    assert isInt(M), "M must be an int"
    M = int(M)

    K = sys.argv[3]
    assert isInt(K), "K must be an int"
    K = int(K)

    L = sys.argv[4]
    assert isInt(L), "L must be an int"
    L = int(L)

    data_path = sys.argv[1]
    

    print(data_path, f"M={M} K={K} L={L}")

    rawData = sc.textFile(data_path).repartition(numPartitions=L).cache()

    split_RDD = rawData.map(lambda x: x.split(','))
    inputPoints = split_RDD.map(lambda x: (float(x[0]), float(x[1])))
    inputPoints = inputPoints.repartition(numPartitions=L).cache()

    total_points = rawData.count()
    # Assuming each line represents a point
    print("Number of points =", total_points)

    D = MRFFT(inputPoints, K, sc)
    start_exact = time.time()
    outliers = MRApproxOutliers(inputPoints, D, M)
    end_exact = time.time()
    print("Running time of MRApproxOutliers =", int((end_exact - start_exact) * 1000), " ms")


if __name__ == "__main__":
    main()
