package com.code.orchestration;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.fnf.model.v20190315.*;
import com.aliyuncs.profile.DefaultProfile;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AliServerlessWorkflow {

    // Create DefaultAcsClient
    static DefaultProfile profile = DefaultProfile.getProfile(
            "***",          // regionID
            "***",      // RAM AccessKey ID
            "****"); // RAM Access Key Secret
    static IAcsClient client = new DefaultAcsClient(profile);
    //configure roleArn in serverless workflow
    static String roleArn = "acs:ram::****";
    //create serverless functions used in serverless workflow
    static String taskResourceArn = "acs:fc:*******";


    //create workflow define model of sequence serverless workflow
    public static void createSequenceFlow(String flowName, String flowDesc, int functionNum) {
        CreateFlowRequest request = new CreateFlowRequest();
        String flowDef = "version: v1\ntype: flow\nsteps:";
        for (int i = 0; i < functionNum; i++) {
            String tmp = "\n  - type: task\n    name: hello" + i + "\n    resourceArn: " + taskResourceArn;
            flowDef += tmp;
        }


        request.setName(flowName);
        request.setDefinition(flowDef);
        request.setType("FDL");
        request.setDescription(flowDesc);
        request.setRoleArn(roleArn);
        try {
            System.out.println(client.getAcsResponse(request));
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    //create workflow define model of parallel serverless workflow
    public static void createParallelFlow(String flowName, String flowDesc, int functionNum) {
        CreateFlowRequest request = new CreateFlowRequest();
        String flowDef = "version: v1\ntype: flow\nsteps:\n  - type: parallel\n    name: step1\n    branches:";
        for (int i = 0; i < functionNum; i++) {
            String tmp = "\n      - steps:\n        - type: task\n          name: hello" + i + "\n          resourceArn: " + taskResourceArn;
            flowDef += tmp;
        }

        request.setName(flowName);
        request.setDefinition(flowDef);
        request.setType("FDL");
        request.setDescription(flowDesc);
        request.setRoleArn(roleArn);
        try {
            System.out.println(client.getAcsResponse(request));
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    //execute serverless workflows
    public static void startExecution(String flowName, String execName, String inputContent) {
        StartExecutionRequest request = new StartExecutionRequest();
        request.setFlowName(flowName);
        request.setExecutionName(execName);
        request.setInput(inputContent);
        try {
            System.out.println(client.getAcsResponse(request));
        } catch (ClientException e) {
            e.printStackTrace();
        }

    }

    //delete serverless workflow
    public static void deleteFlow(String flowName) {
        DeleteFlowRequest request = new DeleteFlowRequest();
        request.setName(flowName);
        try {
            System.out.println(client.getAcsResponse(request));
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    //get log information
    public static GetExecutionHistoryResponse getExecutionHistory(String flowName, String execName) {
        GetExecutionHistoryRequest request = new GetExecutionHistoryRequest();
        request.setFlowName(flowName);
        request.setExecutionName(execName);
        request.setLimit(1000);
        GetExecutionHistoryResponse response = null;
        try {
            response = client.getAcsResponse(request);
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return response;
    }

    //extract log information about time for sequence serverless workflow
    public static void getLogSequenceInfo(String flowName, String execName) {
        GetExecutionHistoryResponse response = getExecutionHistory(flowName, execName);
        //save log information
        ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();
        for (GetExecutionHistoryResponse.EventsItem event : response.getEvents()) {

            logStreamEntity stream = new logStreamEntity();
            stream.setStreamid(event.getEventId() + "");
            stream.setStreamtype(event.getType());
            stream.setStreamtime(event.getTime());
            stream.setStreamnext(event.getScheduleEventId() + "");
            list.add(stream);

        }

        //calculate the total execution time by the start time and the end time
        String stateStartTime = list.get(0).getStreamtime();
        String stateEndTime = list.get(list.size() - 1).getStreamtime();

        long stateTotalTime = getTimeInterval(stateStartTime, stateEndTime);
        //calculate the function execution time
        long funDuration = 0;
        for (logStreamEntity stream : list) {
            if (stream.getStreamtype().equals("TaskSucceeded")) {
                String nextStr = stream.getStreamnext();
                for (int i = 0; i < list.size(); i++) {
                    if (list.get(i).getStreamid().equals(nextStr)) {
                        String endtime = stream.getStreamtime();
                        String starttime = list.get(i).getStreamtime();
                        funDuration += getTimeInterval(starttime, endtime);

                    }
                }
            }
        }
        //calculate the orchestration overhead of workflows
        long time = stateTotalTime - funDuration;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time;
        // output time result
        System.out.println(dataFormat);


    }

    //extract log information about time for parallel serverless workflow
    public static void getLogParallelInfo(String flowName, String execName) {
        GetExecutionHistoryResponse response = getExecutionHistory(flowName, execName);

        ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();
        for (GetExecutionHistoryResponse.EventsItem event : response.getEvents()) {

            logStreamEntity stream = new logStreamEntity();
            stream.setStreamid(event.getEventId() + "");
            stream.setStreamtype(event.getType());
            stream.setStreamtime(event.getTime());
            stream.setStreamnext(event.getScheduleEventId() + "");
            list.add(stream);

        }


        String stateStartTime = list.get(0).getStreamtime();
        String stateEndTime = list.get(list.size() - 1).getStreamtime();

        long stateTotalTime = getTimeInterval(stateStartTime, stateEndTime);

        long funDuration = 0;

        String starttime = "";
        String endtime = "";

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getStreamtype().equals("TaskStarted")) {
                starttime = list.get(i).getStreamtime();
                break;
            }
        }

        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).getStreamtype().equals("TaskSucceeded")) {
                endtime = list.get(i).getStreamtime();
                break;
            }
        }

        funDuration = getTimeInterval(starttime, endtime);


        long time = stateTotalTime - funDuration;


        String loginfo = "stateTotalTime:" + stateTotalTime + ",funDuration:" + funDuration + ",transitionTime:" + time;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 1000);

        System.out.println(dataFormat);

    }

    //calculate the time interval between two date
    public static long getTimeInterval(String startTimeStr, String endTimeStr) {

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");

        Date startDate = null;
        Date endDate = null;

        //solve some special cases to consider start time and end time at the same time.
        // such as eventid: 49 type: TaskStarted time: 2020-06-16T08:43:12Z previousid: 48
        if (startTimeStr.length() == 20) {
            StringBuffer sb = new StringBuffer();
            startTimeStr = sb.append(startTimeStr).insert(startTimeStr.length() - 1, ".000").toString();

        }
        if (endTimeStr.length() == 20) {
            StringBuffer sb = new StringBuffer();
            endTimeStr = sb.append(endTimeStr).insert(endTimeStr.length() - 1, ".000").toString();
            //System.out.println("改造后"+endTimeStr);
        }
        //special cases, such as:2020-06-16T12:30:47.9Z
        if (startTimeStr.split("\\.")[1].length() == 2) {
            StringBuffer sb = new StringBuffer();
            startTimeStr = sb.append(startTimeStr).insert(startTimeStr.length() - 1, "00").toString();

        }
        if (endTimeStr.split("\\.")[1].length() == 2) {
            StringBuffer sb = new StringBuffer();
            endTimeStr = sb.append(endTimeStr).insert(endTimeStr.length() - 1, "00").toString();


        }
        //special cases, such as 2020-05-29T08:28:32.11Z

        if (startTimeStr.split("\\.")[1].length() == 3) {
            StringBuffer sb = new StringBuffer();
            startTimeStr = sb.append(startTimeStr).insert(startTimeStr.length() - 1, "0").toString();

        }
        if (endTimeStr.split("\\.")[1].length() == 3) {
            StringBuffer sb = new StringBuffer();
            endTimeStr = sb.append(endTimeStr).insert(endTimeStr.length() - 1, "0").toString();


        }
        try {
            startDate = inputFormat.parse(startTimeStr);
            endDate = inputFormat.parse(endTimeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //get milliseconds represented start date and end time
        long firstDateMilliSeconds = startDate.getTime();
        long secondDateMilliSeconds = endDate.getTime();

        //get the time interval
        long interval = secondDateMilliSeconds - firstDateMilliSeconds;
        return interval;

    }

    public static void main(String[] args) throws InterruptedException {

        //sequence workflows
        int i = 120;
        createSequenceFlow("AliSeqNoPayload" + i, "sequence" + i + "-test", i);
        for (int j = 1; j <= 24; j++) {
            System.out.print(j + "----");
            startExecution("AliSeqNoPayload" + i, "runNew" + j, "{ }");
            TimeUnit.SECONDS.sleep(170);
        }
        for (int j = 1; j <= 24; j++) {
            getLogSequenceInfo("AliSeqNoPayload" + i, "runNew" + j);
        }


        //parallel
        //int i=120;
        createParallelFlow("AliParNoPayload" + i, "parallel" + i + "-test", i);

        for (int j = 1; j <= 24; j++) {
            System.out.print(j + "----");
            startExecution("AliParNoPayload" + i, "runNew" + j, "{ }");
            TimeUnit.SECONDS.sleep(70);
        }
        for (int j = 1; j <= 24; j++) {
            getLogParallelInfo("AliParNoPayload" + i, "runNew" + j);
        }

    }

}
