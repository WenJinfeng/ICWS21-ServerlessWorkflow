package com.code.orchestrationWorkload;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class AzureWorkloadExperiment {


    public static String queryInsights(String path, String app, String request, String end_time, String start_time) {
        String QueryTemplate = "az monitor app-insights query --apps %s --analytics-query %s --end-time %s --start-time %s";
        String query = String.format(QueryTemplate, app, request, end_time, start_time);
        StringBuilder result = new StringBuilder();
        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder("cmd.exe", "/c", query);
            processBuilder.directory(new File(path));
            Process process = processBuilder.start();
            InputStream inputstream = process.getInputStream();
            InputStream errorstream = process.getErrorStream();

            BufferedReader inputbufferedReader =
                    new BufferedReader(new InputStreamReader(inputstream, "gbk"));

            BufferedReader errorbufferedReader =
                    new BufferedReader(new InputStreamReader(errorstream, "gbk"));

            String inputrs = null;
            while ((inputrs = inputbufferedReader.readLine()) != null) {
                //System.out.println(inputrs);
                result.append(inputrs).append("\n");
                //Logger.info(inputrs);
            }

            String errorrs = null;
            while ((errorrs = errorbufferedReader.readLine()) != null) {
                //System.out.println(errorrs);
                result.append(errorrs).append("\n");
                //Logger.info(errorrs);
            }
        } catch (IOException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return result.toString();
    }

    public static String parseResult(String result) {
        //System.out.println(result);
        Map<String, String> parseResult = new HashMap<String, String>();
        JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
        JsonArray tables = jsonObject.getAsJsonArray("tables");
        JsonObject request = tables.get(0).getAsJsonObject();
        JsonArray content = request.getAsJsonArray("rows").get(0).getAsJsonArray();
        String valueStr = content.get(0).getAsString();
        //System.out.println(valueStr);
        return valueStr;
    }


    public static ArrayList<String> parseResultList(String result) {
        //System.out.println(result);
        Map<String, String> parseResult = new HashMap<String, String>();
        JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
        JsonArray tables = jsonObject.getAsJsonArray("tables");
        JsonObject request = tables.get(0).getAsJsonObject();
        ArrayList<String> list = new ArrayList<String>();
        int itemCount = request.getAsJsonArray("rows").size();
        for (int i = 0; i < itemCount; i++) {
            JsonArray content = request.getAsJsonArray("rows").get(i).getAsJsonArray();
            String valueStr = content.get(0).getAsString();
            list.add(valueStr);
        }

        return list;
    }


    public static void getSequenceLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String startfunction, String endfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, startfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, endfunction, end_time, start_time);

        String ADFstarttime = parseResult(query1);
        String ADFendtime = parseResult(query2);
        ADFstarttime = ADFstarttime.substring(0, 23);
        ADFendtime = ADFendtime.substring(0, 23);

        //String query1 = queryInsights(path, appid, startfunction, end_time, start_time);
        ArrayList<String> liststart = parseResultList(query3);
        //String query2 = queryInsights(path, appid, endfunction, end_time, start_time);
        ArrayList<String> listend = parseResultList(query4);


        ArrayList<String> liststartnew = new ArrayList<String>();
        ArrayList<String> listendnew = new ArrayList<String>();

        for (int i = 0; i < liststart.size(); i++) {
            liststartnew.add(liststart.get(i).substring(0, 23));
        }

        for (int i = 0; i < listend.size(); i++) {
            listendnew.add(listend.get(i).substring(0, 23));
        }


        long funDuration = 0;
        for (int i = 0; i < liststart.size(); i++) {
            funDuration += getTimeInterval(liststartnew.get(i), listendnew.get(i));
            //System.out.println(funDuration);
        }


        long stateTotalTime = getTimeInterval(ADFstarttime, ADFendtime);//总执行时间

        double time = stateTotalTime - funDuration;


        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time;

        System.out.println(dataFormat);


    }


    public static void getParallelLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String startfunction, String endfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, startfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, endfunction, end_time, start_time);

        String ADFstarttime = parseResult(query1);
        String ADFendtime = parseResult(query2);
        String startfunctiontime = parseResult(query3);
        String endfunctiontime = parseResult(query4);


        ADFstarttime = ADFstarttime.substring(0, 23);
        ADFendtime = ADFendtime.substring(0, 23);
        startfunctiontime = startfunctiontime.substring(0, 23);
        endfunctiontime = endfunctiontime.substring(0, 23);


        long funDuration = getTimeInterval(startfunctiontime, endfunctiontime);//函数执行时间

        long stateTotalTime = getTimeInterval(ADFstarttime, ADFendtime);//总执行时间

        double time = stateTotalTime - funDuration;


        String loginfo = "stateTotalTime:" + stateTotalTime + ",funDuration:" + funDuration + ",transitionTime:" + time;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 1000);

        System.out.println(dataFormat);

    }


    public static long getTimeInterval(String startTimeStr, String endTimeStr) {

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS");

        Date startDate = null;
        Date endDate = null;


        try {
            startDate = inputFormat.parse(startTimeStr);
            endDate = inputFormat.parse(endTimeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }


        long firstDateMilliSeconds = startDate.getTime();
        long secondDateMilliSeconds = endDate.getTime();


        long interval = secondDateMilliSeconds - firstDateMilliSeconds;
        return interval;

    }

    public static String testStartOperator(String surl) throws IOException, JSONException {

        URL url = new URL(surl);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        InputStreamReader input = new InputStreamReader(httpConn
                .getInputStream(), "utf-8");
        BufferedReader bufReader = new BufferedReader(input);
        String line = "";
        StringBuilder contentBuf = new StringBuilder();
        while ((line = bufReader.readLine()) != null) {
            contentBuf.append(line);
        }
        String buf = contentBuf.toString();
        System.out.println(buf);
        JSONObject jsonObject = new JSONObject(buf);
        return (String) jsonObject.get("statusQueryGetUri");

    }

    public static void testResultOperator(String surl, String outputFile) throws IOException, JSONException, ParseException {

        URL url = new URL(surl);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        InputStreamReader input = new InputStreamReader(httpConn
                .getInputStream(), "utf-8");
        BufferedReader bufReader = new BufferedReader(input);
        String line = "";
        StringBuilder contentBuf = new StringBuilder();
        while ((line = bufReader.readLine()) != null) {
            contentBuf.append(line);
        }
        String buf = contentBuf.toString();
        System.out.println(buf);
        JSONObject jsonObject = new JSONObject(buf);
        String instanceId = (String) jsonObject.get("instanceId");
        String createdTime = (String) jsonObject.get("createdTime");
        String lastUpdatedTime = (String) jsonObject.get("lastUpdatedTime");


        createdTime = createdTime.substring(0, 19) + ".00Z";


        lastUpdatedTime = transferData(lastUpdatedTime);


        BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputFile, true), "utf-8"));

        bw.newLine();
        bw.write(instanceId + "," + createdTime + "," + lastUpdatedTime);
        System.out.println(instanceId + "," + createdTime + "," + lastUpdatedTime);
        bw.close();

    }


    public static String transferData(String timestr) {
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

        Date createdTimeDate = null;
        try {
            createdTimeDate = inputFormat.parse(timestr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(createdTimeDate);
        cal.add(Calendar.SECOND, 2);
        createdTimeDate = cal.getTime();

        String writestr = inputFormat.format(createdTimeDate).substring(0, 19) + ".00Z";
        System.out.println("after:" + writestr);
        return writestr;

    }


    public static void getMapReduceLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String generatedatastartfunction, String generatedataendfunction, String mapstartfunction, String mapendfunction, String reducestartfunction, String reduceendfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, generatedatastartfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, generatedataendfunction, end_time, start_time);

        String query5 = queryInsights(path, appid, mapstartfunction, end_time, start_time);

        String query6 = queryInsights(path, appid, mapendfunction, end_time, start_time);

        String query7 = queryInsights(path, appid, reducestartfunction, end_time, start_time);

        String query8 = queryInsights(path, appid, reduceendfunction, end_time, start_time);


        String ADFstarttime = parseResult(query1);
        String ADFendtime = parseResult(query2);
        ADFstarttime = ADFstarttime.substring(0, 23);
        ADFendtime = ADFendtime.substring(0, 23);


        String starttime3 = parseResult(query3);
        String endtime4 = parseResult(query4);
        starttime3 = starttime3.substring(0, 23);
        endtime4 = endtime4.substring(0, 23);

        String starttime5 = parseResult(query5);
        String endtime6 = parseResult(query6);
        starttime5 = starttime5.substring(0, 23);
        endtime6 = endtime6.substring(0, 23);

        String starttime7 = parseResult(query7);
        String endtime8 = parseResult(query8);
        starttime7 = starttime7.substring(0, 23);
        endtime8 = endtime8.substring(0, 23);

        long stateTotalTime = getTimeInterval(ADFstarttime, ADFendtime);//总执行时间
        long generatedataTime = getTimeInterval(starttime3, endtime4);
        long mapTime = getTimeInterval(starttime5, endtime6);
        long reduceTime = getTimeInterval(starttime7, endtime8);

        long funDuration = generatedataTime + mapTime + reduceTime;

        long time = stateTotalTime - funDuration;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + mapTime + "\t" + reduceTime;

        System.out.println(dataFormat);


    }

    public static void getKMeansLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String generatedatastartfunction, String generatedataendfunction, String randomstartfunction, String randomendfunction, String kmeansstartfunction, String kmeansendfunction, String outputstartfunction, String outputendfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, generatedatastartfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, generatedataendfunction, end_time, start_time);

        String query5 = queryInsights(path, appid, randomstartfunction, end_time, start_time);

        String query6 = queryInsights(path, appid, randomendfunction, end_time, start_time);

        String query7 = queryInsights(path, appid, kmeansstartfunction, end_time, start_time);

        String query8 = queryInsights(path, appid, kmeansendfunction, end_time, start_time);

        String query9 = queryInsights(path, appid, outputstartfunction, end_time, start_time);

        String query10 = queryInsights(path, appid, outputendfunction, end_time, start_time);


        String ADFstarttime = parseResult(query1);
        String ADFendtime = parseResult(query2);
        ADFstarttime = ADFstarttime.substring(0, 23);
        ADFendtime = ADFendtime.substring(0, 23);


        String starttime3 = parseResult(query3);
        String endtime4 = parseResult(query4);
        starttime3 = starttime3.substring(0, 23);
        endtime4 = endtime4.substring(0, 23);

        String starttime5 = parseResult(query5);
        String endtime6 = parseResult(query6);
        starttime5 = starttime5.substring(0, 23);
        endtime6 = endtime6.substring(0, 23);

        String starttime7 = parseResult(query7);
        String endtime8 = parseResult(query8);
        starttime7 = starttime7.substring(0, 23);
        endtime8 = endtime8.substring(0, 23);

        String starttime9 = parseResult(query9);
        String endtime10 = parseResult(query10);
        starttime9 = starttime9.substring(0, 23);
        endtime10 = endtime10.substring(0, 23);


        long stateTotalTime = getTimeInterval(ADFstarttime, ADFendtime);//总执行时间


        long generatedataTime = getTimeInterval(starttime3, endtime4);
        long randomTime = getTimeInterval(starttime5, endtime6);
        long kmeansTime = getTimeInterval(starttime7, endtime8);
        long outputTime = getTimeInterval(starttime9, endtime10);

        long funDuration = generatedataTime + randomTime + kmeansTime + outputTime;

        long time = stateTotalTime - funDuration;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + randomTime + "\t" + kmeansTime + "\t" + outputTime;
        System.out.println(dataFormat);


    }

    public static void main(String[] args) throws IOException, JSONException, ParseException, InterruptedException {

//        String path = "*********";
//        String appid = "*********";
//        String appname = "MapReduceExperiment";
//        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
//        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";
//
//        String generatedatastartfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
//        String generatedataendfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";
//
//        String mapstartfunction = "\"traces | where message contains 'Function \\'Mapper (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
//        String mapendfunction = "\"traces | where message contains 'Function \\'Mapper (Activity)\\' completed' | sort by timestamp asc |summarize max(timestamp)\"";
//
//        String reducestartfunction = "\"traces | where message contains 'Function \\'Reduce (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
//        String reduceendfunction = "\"traces | where message contains 'Function \\'Reduce (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";
//
//        String start_time="2020-08-25T13:28:21.00Z";
//        String end_time="2020-08-25T13:28:24.00Z";
//        getMapReduceLogInfo(path, appid, startorchestrator, endorchestrator, generatedatastartfunction, generatedataendfunction, mapstartfunction,mapendfunction,reducestartfunction,reduceendfunction,end_time, start_time, appname);




/*
        String path = "*********";
        String appid = "*********";
        String appname = "KMeansExperiment";
        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";

        String generatedatastartfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String generatedataendfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String randomstartfunction = "\"traces | where message contains 'Function \\'GenerateCentroids (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String randomendfunction = "\"traces | where message contains 'Function \\'GenerateCentroids (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String kmeansstartfunction = "\"traces | where message contains 'Function \\'KMeans (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String kmeansendfunction = "\"traces | where message contains 'Function \\'KMeans (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String outputstartfunction = "\"traces | where message contains 'Function \\'OutputResult (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String outputendfunction = "\"traces | where message contains 'Function \\'OutputResult (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String start_time="2020-08-25T13:01:06.00Z";
        String end_time="2020-08-25T13:01:10.00Z";

        getKMeansLogInfo(path, appid, startorchestrator, endorchestrator, generatedatastartfunction, generatedataendfunction, randomstartfunction,randomendfunction,kmeansstartfunction,kmeansendfunction, outputstartfunction,outputendfunction,end_time,start_time, appname);


*/


//
//        String appname= "*********";
//        String url="*********";
//        String outputFile="*********";
//        for(int j=1;j<=26;j++){
//            try{
//                TimeUnit.SECONDS.sleep(120);
//                System.out.println("start:"+j);
//                String stateurl=testStartOperator(url);
//                TimeUnit.SECONDS.sleep(40);
//                testResultOperator(stateurl,outputFile);
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (JSONException e) {
//                e.printStackTrace();
//            } catch (ParseException e) {
//                e.printStackTrace();
//            }
//
//        }


       /* String appname= "nKMeansExperiment";
        String url="*********";
        String outputFile="*********";
        for(int j=1;j<=26;j++){
            try{
                TimeUnit.SECONDS.sleep(40);
                System.out.println("start:"+j);
                String stateurl=testStartOperator(url);
                TimeUnit.SECONDS.sleep(40);
                testResultOperator(stateurl,outputFile);

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
*/

/*
        //获取结果
        String path = "*********";
        String appid = "*********";
        String appname = "MapReduceExperiment";

        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";

        String generatedatastartfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String generatedataendfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String mapstartfunction = "\"traces | where message contains 'Function \\'Mapper (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String mapendfunction = "\"traces | where message contains 'Function \\'Mapper (Activity)\\' completed' | sort by timestamp asc |summarize max(timestamp)\"";

        String reducestartfunction = "\"traces | where message contains 'Function \\'Reduce (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String reduceendfunction = "\"traces | where message contains 'Function \\'Reduce (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String inputFile="*********";
        //读文件
        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        String cellinfo = "";
        while ((cellinfo = br.readLine()) != null) {
            String[] timedata = cellinfo.split(",");
            String start_time = timedata[1];
            String end_time = timedata[2];
            TimeUnit.SECONDS.sleep(3);
            try{
                getMapReduceLogInfo(path, appid, startorchestrator, endorchestrator, generatedatastartfunction, generatedataendfunction, mapstartfunction,mapendfunction,reducestartfunction,reduceendfunction,end_time, start_time, appname);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        br.close();
        fr.close();

*/

        //获取结果
        String path = "*********";
        String appid = "*********";
        String appname = "nKMeansExperiment";

        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";

        String generatedatastartfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String generatedataendfunction = "\"traces | where message contains 'Function \\'GenerateData (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String randomstartfunction = "\"traces | where message contains 'Function \\'GenerateCentroids (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String randomendfunction = "\"traces | where message contains 'Function \\'GenerateCentroids (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String kmeansstartfunction = "\"traces | where message contains 'Function \\'KMeans (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String kmeansendfunction = "\"traces | where message contains 'Function \\'KMeans (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String outputstartfunction = "\"traces | where message contains 'Function \\'OutputResult (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String outputendfunction = "\"traces | where message contains 'Function \\'OutputResult (Activity)\\' completed' | sort by timestamp asc |summarize min(timestamp)\"";

        String inputFile = "*********";

        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        String cellinfo = "";
        while ((cellinfo = br.readLine()) != null) {
            String[] timedata = cellinfo.split(",");
            String start_time = timedata[1];
            String end_time = timedata[2];
            TimeUnit.SECONDS.sleep(3);
            try {
                getKMeansLogInfo(path, appid, startorchestrator, endorchestrator, generatedatastartfunction, generatedataendfunction, randomstartfunction, randomendfunction, kmeansstartfunction, kmeansendfunction, outputstartfunction, outputendfunction, end_time, start_time, appname);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        br.close();
        fr.close();

    }

}
