package com.code.orchestrationTime;


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


public class ADFTimeExperiment {
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

    public static String parseResult(String result) {

        Map<String, String> parseResult = new HashMap<String, String>();
        JsonObject jsonObject = new JsonParser().parse(result).getAsJsonObject();
        JsonArray tables = jsonObject.getAsJsonArray("tables");
        JsonObject request = tables.get(0).getAsJsonObject();
        JsonArray content = request.getAsJsonArray("rows").get(0).getAsJsonArray();
        String valueStr = content.get(0).getAsString();

        return valueStr;
    }

    //存多条返回结果
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


    public static void getSequenceLogInfo(String path, String appid, String startorchestrator, String endorchestrator, String startfunction, String endfunction, String end_time, String start_time, String appname) {

        String query1 = queryInsights(path, appid, startorchestrator, end_time, start_time);

        String query2 = queryInsights(path, appid, endorchestrator, end_time, start_time);

        String query3 = queryInsights(path, appid, startfunction, end_time, start_time);

        String query4 = queryInsights(path, appid, endfunction, end_time, start_time);

        String ADFstarttime = parseResult(query1);
        String ADFendtime = parseResult(query2);
        ADFstarttime = ADFstarttime.substring(0, 23);
        ADFendtime = ADFendtime.substring(0, 23);


        ArrayList<String> liststart = parseResultList(query3);

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

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 10000);

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

    public static void main(String[] args) throws IOException, InterruptedException {

        //--------------------------------------------------
        //sequence
        /*String appname= "ADFSequence5Time120";
        String url="********";
        String outputFile="********";
        for(int j=1;j<=26;j++){
            try{
                TimeUnit.SECONDS.sleep(60);
                System.out.println("start:"+j);
                String stateurl=testStartOperator(url);
                TimeUnit.SECONDS.sleep(700);
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


//                //获取结果
       /* String path = "********";
        String appid = "********";
        String appname = "ADFSequence5Time120";
        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";
        String startfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' started' | sort by timestamp asc | project timestamp\"";
        String endfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' completed' | sort by timestamp asc | project timestamp\"";

        String inputFile="********";
        //读文件
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
        fr.close();*/


        //parallel

//        String appname = "ADFParallel5Time10";
//        String url = "********";
//        String outputFile = "********";
//        for (int j = 1; j <= 26; j++) {
//            try {
//                TimeUnit.SECONDS.sleep(40);
//                System.out.println("start:" + j);
//                String stateurl = testStartOperator(url);
//                TimeUnit.SECONDS.sleep(60);
//                testResultOperator(stateurl, outputFile);
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


        String path = "********";
        String appid = "********";
        String appname = "ADFParallel5Time10";
        String startorchestrator = "\"traces | where message contains 'Executing \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize min(timestamp)\"";
        String endorchestrator = "\"traces | where message contains 'Executed \\'Functions.HelloOrchestrator\\' ' | sort by timestamp asc |summarize max(timestamp)\"";
        String startfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' started' | sort by timestamp asc |summarize min(timestamp)\"";
        String endfunction = "\"traces | where message contains 'Function \\'Hello (Activity)\\' completed' | sort by timestamp asc |summarize max(timestamp)\"";

        String inputFile = "********";
        //读文件
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
