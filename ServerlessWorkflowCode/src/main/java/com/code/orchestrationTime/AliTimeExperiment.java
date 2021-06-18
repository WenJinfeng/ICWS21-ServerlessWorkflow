package com.code.orchestrationTime;


import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.fnf.model.v20190315.CreateFlowRequest;
import com.aliyuncs.fnf.model.v20190315.GetExecutionHistoryRequest;
import com.aliyuncs.fnf.model.v20190315.GetExecutionHistoryResponse;
import com.aliyuncs.fnf.model.v20190315.StartExecutionRequest;
import com.aliyuncs.profile.DefaultProfile;
import com.code.orchestration.logStreamEntity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


public class AliTimeExperiment {
    // Create DefaultAcsClient
    static DefaultProfile profile = DefaultProfile.getProfile(
            "********",
            "********",
            "********");
    static IAcsClient client = new DefaultAcsClient(profile);

    static String roleArn = "acs:ram::********";


    public static void createSequenceFlow(String taskResourceArn, String flowName, String flowDesc, int functionNum) {
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

    public static void getLogSequenceInfo(String flowName, String execName) {
        GetExecutionHistoryResponse response = getExecutionHistory(flowName, execName);

        ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();
        for (GetExecutionHistoryResponse.EventsItem event : response.getEvents()) {

            //System.out.printf("eventid: %s type: %s time: %s previousid: %s %n", event.getEventId(), event.getType(), event.getTime(), event.getScheduleEventId());
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

        long time = stateTotalTime - funDuration;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time;

        System.out.println(dataFormat);

    }

    public static long getTimeInterval(String startTimeStr, String endTimeStr) {

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");

        Date startDate = null;
        Date endDate = null;

        if (startTimeStr.length() == 20) {
            StringBuffer sb = new StringBuffer();
            startTimeStr = sb.append(startTimeStr).insert(startTimeStr.length() - 1, ".000").toString();

        }
        if (endTimeStr.length() == 20) {
            StringBuffer sb = new StringBuffer();
            endTimeStr = sb.append(endTimeStr).insert(endTimeStr.length() - 1, ".000").toString();

        }


        if (startTimeStr.split("\\.")[1].length() == 2) {
            StringBuffer sb = new StringBuffer();
            startTimeStr = sb.append(startTimeStr).insert(startTimeStr.length() - 1, "00").toString();

        }
        if (endTimeStr.split("\\.")[1].length() == 2) {
            StringBuffer sb = new StringBuffer();
            endTimeStr = sb.append(endTimeStr).insert(endTimeStr.length() - 1, "00").toString();


        }

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


        long firstDateMilliSeconds = startDate.getTime();
        long secondDateMilliSeconds = endDate.getTime();


        long interval = secondDateMilliSeconds - firstDateMilliSeconds;
        return interval;

    }


    public static void createParallelFlow(String taskResourceArn, String flowName, String flowDesc, int functionNum) {
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


        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 120000);

        System.out.println(dataFormat);

    }

    public static void main(String[] args) throws InterruptedException {

        String taskResouceArn50ms = "acs:fc:********";
        String taskResourceArn100ms = "acs:fc:********";
        String taskResourceArn10 = "acs:fc:********";
        String taskResourceArn20 = "acs:fc:********";
        String taskResourceArn40 = "acs:fc:********";
        String taskResourceArn60 = "acs:fc:********";
        String taskResourceArn120 = "acs:fc:********";


        //sequence
        String time = "60";
        createSequenceFlow(taskResourceArn120, "AliSequence5Time" + time, "sequence5-test", 5);


        for (int j = 1; j <= 25; j++) {
            //System.out.print(j+"----");
            startExecution("AliSequence5Time" + time, "TimeRun" + j, "{ }");
            //TimeUnit.SECONDS.sleep(700);
        }
        for (int j = 1; j <= 25; j++) {
            getLogSequenceInfo("AliSequence5Time" + time, "TimeRun" + j);
        }


        for (int j = 1; j <= 25; j++) {
            //System.out.print(j+"----");
            startExecution("AliSeqNoPayload5", "TimeRunNewNew" + j, "{ }");
            //TimeUnit.SECONDS.sleep(40);
            getLogSequenceInfo("AliSeqNoPayload5", "TimeRunNewNew" + j);
        }


        //parallel
        //String time="120";
        createParallelFlow(taskResourceArn120, "AliParallel5Time" + time, "parallel5-test", 5);

        for (int j = 1; j <= 25; j++) {
            //System.out.print(j+"----");
            startExecution("AliParallel5Time" + time, "TimeRunNew" + j, "{ }");
            //TimeUnit.SECONDS.sleep(260);
        }
        for (int j = 1; j <= 25; j++) {
            getLogParallelInfo("AliParallel5Time" + time, "TimeRunNew" + j);
        }
    }
}
