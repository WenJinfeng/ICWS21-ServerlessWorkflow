package com.code.orchestration;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;

import java.io.*;
import java.util.concurrent.TimeUnit;


public class GoogleCloudComposer {

    public static void getLog() throws IOException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        MetricServiceSettings metricServiceSettings =
                MetricServiceSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .build();
        MetricServiceClient metricServiceClient = MetricServiceClient.create(metricServiceSettings);
        long startMillis = System.currentTimeMillis() - ((60 * 20) * 1000);
        TimeInterval interval =
                TimeInterval.newBuilder()
                        .setStartTime(Timestamps.fromMillis(startMillis))
                        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();

        ListTimeSeriesRequest.Builder requestBuilder =
                ListTimeSeriesRequest.newBuilder()
                        .setName("projects/cynthiaproject")
                        .setFilter("metric.type=\"composer.googleapis.com/workflow/run_duration\"")
                        .setInterval(interval)
                        .setView(ListTimeSeriesRequest.TimeSeriesView.HEADERS);

        ListTimeSeriesRequest request = requestBuilder.build();

        MetricServiceClient.ListTimeSeriesPagedResponse response = metricServiceClient.listTimeSeries(request);
    }

    public static String queryexe(String path, String query) {
        StringBuilder result = new StringBuilder();
        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder("cmd.exe", "/c", query);
            processBuilder.directory(new File(path));
            Process process = processBuilder.start();
            InputStream inputstream = process.getInputStream();
            InputStream errorstream = process.getErrorStream();
            // Windows中文操作系统此处要使用gbk转码，否则输出会乱码
            BufferedReader inputbufferedReader =
                    new BufferedReader(new InputStreamReader(inputstream, "gbk"));

            BufferedReader errorbufferedReader =
                    new BufferedReader(new InputStreamReader(errorstream, "gbk"));

            String inputrs = null;
            while ((inputrs = inputbufferedReader.readLine()) != null) {
                System.out.println(inputrs);
                result.append(inputrs).append("\n");
                //Logger.info(inputrs);
            }

            String errorrs = null;
            while ((errorrs = errorbufferedReader.readLine()) != null) {
                System.out.println(errorrs);
                result.append(errorrs).append("\n");
                //Logger.info(errorrs);
            }
        } catch (IOException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return result.toString();
    }

    /*
       gcloud beta composer environments create test-environment \
            --location us-central1 \
            --zone us-central1-f \
            --machine-type n1-standard-2 \
            --image-version composer-latest-airflow-x.y.z \
            --labels env=beta
     */
//create environment.
    public static void createEnv(String path, String envname) {
        String standard = "gcloud composer environments create %s";
        String query = String.format(standard, envname);
        queryexe(path, query);
    }

    /*
     gcloud composer environments delete ENVIRONMENT_NAME \
            --location LOCATION
     */
//delete environment.
    public static void deleteEnv(String path, String envname) {
        String standard = "gcloud composer environments delete %s --location us-central1";
        String query = String.format(standard, envname);
        queryexe(path, query);
    }

    /*
        gcloud composer environments storage dags import \
            --environment ENVIRONMENT_NAME \
            --location LOCATION \
            --source LOCAL_FILE_TO_UPLOAD
        gcloud composer environments storage dags import --environment env-1 --location us-central1 --source F:/example_subdag_operator.py
     */
//import dag-python
    public static void createDagfromFile(String path, String envname, String sourcefile) {
        String standard = "gcloud composer environments storage dags import --environment %s --location us-west3 --source %s";
        String query = String.format(standard, envname, sourcefile);
        queryexe(path, query);
    }

    /*
    gcloud composer environments run env-1 trigger_dag -- dag_id
     */
//run dag
    public static void runDag(String path, String envname, String dagid) {
        String stardard = "gcloud composer environments run %s trigger_dag -- %s";
        String query = String.format(stardard, envname, dagid);
        queryexe(path, query);
    }

    /*
        gcloud composer environments storage dags delete

            --environment ENVIRONMENT_NAME

            --location LOCATION

            DAG_NAME.py

     */

    //delete dag
    public static void deleteDag(String path, String envname, String filename) throws IOException {
        String standard = "gcloud composer environments storage dags delete --environment %s --location us-central1 %s";
        String query = String.format(standard, envname, filename);
        CmdTest(path, query);
    }

    //cmd commend
    public static String CmdTest(String path, String query) throws IOException {
        StringBuilder result = new StringBuilder();
        ProcessBuilder processBuilder =
                new ProcessBuilder("cmd.exe", "/c", query);
        processBuilder.directory(new File(path));
        Process process = processBuilder.start();
        new Thread() {
            public void run() {
                try {
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                    bw.write("y");
                    bw.newLine();

                    bw.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }.start();
        ;
        new Thread() {
            public void run() {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String cmdout = "";
                    while ((cmdout = br.readLine()) != null) {
                        System.out.println(cmdout);
                        result.append(cmdout).append("\n");
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }.start();
        return result.toString();
    }


    public static void main(String[] args) throws IOException {

        String path = "*****";
        //queryexe(path,query);
        //createEnv(path,"env-2");
        //deleteEnv(path,"env-1");
        //deleteDag(path, "env-1", "example_subdag_operator.py");

        //创建sequence实验
        for (int i = 1; i <= 25; i++) {

            try {
                System.out.println(i + "--");
                createDagfromFile(path, "newtest", "pyfile");
                System.out.println("File upload finished!");
                TimeUnit.SECONDS.sleep(90);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {

            }
        }


    }
}
