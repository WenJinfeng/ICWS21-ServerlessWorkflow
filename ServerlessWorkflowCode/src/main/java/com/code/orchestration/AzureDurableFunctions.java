package com.code.orchestration;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.json.JSONException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class AzureDurableFunctions {
    //use cmd to search log information (ADF is deployed through VS code tools (https://docs.microsoft.com/en-us/azure/azure-functions/durable/quickstart-js-vscode))
    //
    //in cmd way, first "az login" to jump the explore to login in Azure account.
    //
    //get log information
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

                result.append(inputrs).append("\n");

            }

            String errorrs = null;
            while ((errorrs = errorbufferedReader.readLine()) != null) {

                result.append(errorrs).append("\n");

            }
        } catch (IOException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return result.toString();
    }

    //parse log information
    public static String parseResult(String result) {

        Map<String, String> parseResult = new HashMap<String, String>();
        JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
        JsonArray tables = jsonObject.getAsJsonArray("tables");
        JsonObject request = tables.get(0).getAsJsonObject();
        JsonArray content = request.getAsJsonArray("rows").get(0).getAsJsonArray();
        String valueStr = content.get(0).getAsString();

        return valueStr;
    }

    //form structure log information
    public static ArrayList<String> parseResultList(String result) {

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

    //parallel workflows, get several information
    public static void getParallelLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String startfunction, String endfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, startfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, endfunction, end_time, start_time);

        String ADFstarttime = parseResult(query1); //get start time of the total time
        String ADFendtime = parseResult(query2);//get end time of the total time
        String startfunctiontime = parseResult(query3);//get start time of the functions
        String endfunctiontime = parseResult(query4);//get end time of the functions

        //deal with special case “2020-06-09T08:56:07.5027139Z” to “2020-06-09T08:56:06.346”
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

    //get the time interval of two date
    public static long getTimeInterval(String startTimeStr, String endTimeStr) {

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS");

        Date startDate = null;
        Date endDate = null;

        //deal with special cases
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

    //transfer time by adding 2s as end time
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

    public static void main(String[] args) throws IOException, JSONException, ParseException, InterruptedException {

        //execute ADF about parallel workflows


//        String appname= "ADFParNoPayload120";
//        String url="application url in Azure ******";
//        String outputFile="****save execution information file";
//        for(int j=1;j<=30;j++){
//            try{
//                TimeUnit.SECONDS.sleep(60);
//                System.out.println("start:"+j);
//                String stateurl=testStartOperator(url);
//                TimeUnit.SECONDS.sleep(120);
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

        //get log information by cmd in Windows


     /*   String path = "the root directory about cmd****";
        String appid = "******";
        String appname = "ADFSeqNoPayload120";
        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";
        String startfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' started' | sort by timestamp asc | project timestamp\"";
        String endfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' completed' | sort by timestamp asc | project timestamp\"";

        String inputFile="****** log information file";

        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        String cellinfo = "";
        while ((cellinfo = br.readLine()) != null) {
            String[] timedata = cellinfo.split(",");
            String start_time = timedata[1];
            String end_time = timedata[2];
            TimeUnit.SECONDS.sleep(5);
            try{
                getSequenceLogInfo(path, appid, startorchestrator, endorchestrator, startfunction, endfunction, end_time, start_time, appname);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        br.close();
        fr.close();
*/

//========================================================================
        // execute ADF about parallel workflows

/*        String appname = "ADFParNoPayload120";
        String url = "******";
        String outputFile = "******";
        for (int j = 1; j <= 28; j++) {
            try {
                TimeUnit.SECONDS.sleep(60);
                System.out.println("start:" + j);
                String stateurl = testStartOperator(url);
                TimeUnit.SECONDS.sleep(160);
                testResultOperator(stateurl, outputFile);

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }*/

        //get log information

        String path = "******";
        String appid = "******";
        String appname = "ADFParNoPayload120";
        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";
        String startfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String endfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' completed' | sort by timestamp asc |summarize max(timestamp)\"";

        String inputFile = "******";

        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        String cellinfo = "";
        while ((cellinfo = br.readLine()) != null) {
            String[] timedata = cellinfo.split(",");
            String start_time = timedata[1];
            String end_time = timedata[2];
            TimeUnit.SECONDS.sleep(5);
            try {

                getParallelLogInfo(path, appid, startorchestrator, endorchestrator, startfunction, endfunction, end_time, start_time, appname);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        br.close();
        fr.close();
    }
}
