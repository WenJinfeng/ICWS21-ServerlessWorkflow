# SourceCodeandFigure


# Discussion

For verifying our findings, we conduct experiments of two serverless application workloads, i.e., KMeans and MapReduce. Then, we discuss some limitations of our study.

KMeans application is implemented in a sequence workflow, and accomplishes the clustering functionality for point sets with three-dimensional space. First, use a serverless function to generate 1500 points, because the data payload limit of ASW cannot generate the data of 2000 points. Second, initialize the centroid points randomly. For the KMeans algorithm, the K-value of clustering needs to be given in advance. We adopt Elbow Method to determine K as 8. Next, based on the point set and centroid points, perform the clustering functionality of KMeans. Finally, output and show the clustering result.

![The performance of the KMeans application.](pics/KMeans-compare-totalTime+funTime+overheadTime.pdf)



Figure~\ref{fig:KMeans-compare-totalTime+funTime+overheadTime} represents the comparison of \emph{totalTime}, \emph{funTime}, and \emph{overheadTime} of the \emph{KMeans} application for ASF, ADF, ASW, and GCC. ASF shows the shortest \emph{totalTime} and \emph{overheadTime}, and ADF has the shortest \emph{funTime} (\textbf{F.8 in Table~\ref{tab:findingandimplication}}). This is consistent with the implication \textbf{I.3 in Table~\ref{tab:findingandimplication}}. We also find the same inferred that the changing trend of \emph{totalTime} in sequence workflow is mainly affected by \emph{overheadTime} (\textbf{F.3 in Table~\ref{tab:findingandimplication}}) because \emph{funTime} cost the relatively low and stable time in this \emph{KMeans} application. In terms of data-flow complexity about the data payloads, the previous conclusion (\textbf{I.6 in Table~\ref{tab:findingandimplication}}) is that when the data payload is less than $2^{18}$, ASF is advised to use. In the \emph{KMeans} application, the data payload size is within $2^{18}$, and the performance of ASF is best considering \emph{totalTime}, \emph{overheadTime}. Figure~\ref{fig:KMeans-compare-function} shows execution times of respective functions. Compared with GCC, the execution time of each function of ASF, ADF, and ASW is lower and more stable. It can still be concluded that the performance ADF is the best on the function execution (\textbf{F.8 in Table~\ref{tab:findingandimplication}}).

![The comparison about execution times of respective functions about the KMeans application.](pics/KMeans-compare-function.pdf)



\noindent\textbf{MapReduce application} is implemented in a parallel workflow and is accomplished by the workflow solution example~\cite{MapReduceExample}. The application goal is to generate a batch of data to be processed, the value of data is $data\_1$ or $date\_2$. Count the number of occurrences of various data leveraging \emph{MapReduce} processing frame mode.



\textbf{I.1 in Table~\ref{tab:findingandimplication}} presents that ADF is used in small-scale activity-intensive parallel workflows. In Figure~\ref{fig:MapReduce-compare-totalTime+funTime+overheadTime}, it also shows ADF has the relatively short \emph{totalTime}. However, results from \emph{totalTime} and \emph{overheadTime} of ASF are more stable than ADF. In the \emph{MapReduce} application, there are certain data payload to be transmitted. In the presence of the data payload, the previous conclusion is the ASF is more suitable when data payloads are less than $2^{15}$B in parallel workflow (\textbf{I.6 in Table~\ref{tab:findingandimplication}}). Thus, results of ASF show a relatively satisfactory \emph{totalTime} and \emph{overheadTime}. For respective function execution times, we also find that the performance of ADF is best and the same as our previous conclusions (\textbf{F.8 in Table~\ref{tab:findingandimplication}}). Due to space reasons, the distribution figure about respective execution times of functions is not displayed.


![The performance of the MapReduce application.](pics/MapReduce-compare-totalTime+funTime+overheadTime.pdf)



\noindent\textbf{Limitations.} We discuss the limitations of our study. (i) \textbf{Selection of application scenarios.} Our study is based on sequence and parallel workflows. We may ignore other complex structures, e.g., choice, missing valuable insights with regard to the structure complexity of workflows. In future work, we plan to extend our study to diversify workflow structure to further obtain interesting findings. (ii) \textbf{Experiments of GCC.} In our study, the results of GCC fluctuate greatly, and we suppose it may be related to its environmental setting. To minimize this impact, we repeat several measurements. From the perspective of serverless computing, we suppose that functions performed in DAGs of GCC are not serverless, i.e., GCC is not designed for orchestrating serverless
functions. Until October 2020, we verify our assumption. Beta launch of \emph{Workflows}~\cite{GoogleWorkflow} service is released in the category of serverless computing of Google Cloud. However, its functionality has been immature yet, and we look forward to furthering research in our future work. 
