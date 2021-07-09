
![image](./pics/FunctionNumPar-compare-funTime.pdf)

# SourceCodeandFigure

![image](https://user-images.githubusercontent.com/73005808/122500711-dc1a2380-d025-11eb-85c2-961920235a6c.png)

# Discussion

For verifying our findings, we conduct experiments of two serverless application workloads, i.e., KMeans and MapReduce. 

KMeans application is implemented in a sequence workflow, and accomplishes the clustering functionality for point sets with three-dimensional space. First, use a serverless function to generate 1500 points, because the data payload limit of ASW cannot generate the data of 2000 points. Second, initialize the centroid points randomly. For the KMeans algorithm, the K-value of clustering needs to be given in advance. We adopt Elbow Method to determine K as 8. Next, based on the point set and centroid points, perform the clustering functionality of KMeans. Finally, output and show the clustering result.

![image](https://user-images.githubusercontent.com/73005808/122495758-319e0280-d01d-11eb-9ffb-e0fe6e9dadbe.png)

Figure 1': The performance of the KMeans application.

Figure 1' represents the comparison of totalTime, funTime, and overheadTime of the KMeans application for ASF, ADF, ASW, and GCC. ASF shows the shortest totalTime and overheadTime, and ADF has the shortest funTime (F.8 in Table 1). This is consistent with the implication I.3 in Table 1. We also find the same inferred that the changing trend of totalTime in sequence workflow is mainly affected by overheadTime (F.3 in Table 1) because funTime cost the relatively low and stable time in this KMeans application. In terms of data-flow complexity about the data payloads, the previous conclusion (I.6 in Table 1) is that when the data payload is less than 2^18, ASF is advised to use. In the KMeans application, the data payload size is within 2^18, and the performance of ASF is best considering totalTime, overheadTime. Figure 2' shows execution times of respective functions. Compared with GCC, the execution time of each function of ASF, ADF, and ASW is lower and more stable. It can still be concluded that the performance ADF is the best on the function execution (F.8 in Table 1).

![image](https://user-images.githubusercontent.com/73005808/122495820-4d090d80-d01d-11eb-9a73-c28ddd06b137.png)

Figure 2': The comparison about execution times of respective functions about the KMeans application.


MapReduce application is implemented in a parallel workflow and is accomplished by the workflow solution example (https://github.com/awesome-fnf/ETL-DataProcessing). The application goal is to generate a batch of data to be processed, the value of data is data_1 or date_2. Count the number of occurrences of various data leveraging MapReduce processing frame mode.

I.1 in Table 1 presents that ADF is used in small-scale activity-intensive parallel workflows. In Figure 3', it also shows ADF has the relatively short totalTime. However, results from totalTime and overheadTime of ASF are more stable than ADF. In the MapReduce application, there are certain data payload to be transmitted. In the presence of the data payload, the previous conclusion is the ASF is more suitable when data payloads are less than 2^15B in parallel workflow (I.6 in Table 1). Thus, results of ASF show a relatively satisfactory totalTime and overheadTime. For respective function execution times (Figure 4'), we also find that the performance of ADF is best and the same as our previous conclusions (F.8 in Table 1). 

![image](https://user-images.githubusercontent.com/73005808/122495853-598d6600-d01d-11eb-8634-c215802bb224.png)

Figure 3': The performance of the MapReduce application.

![image](https://user-images.githubusercontent.com/73005808/122496698-c6553000-d01e-11eb-86f8-1e85e6d25055.png)

Figure 4': The comparison about respectively function times of MapReduce.
