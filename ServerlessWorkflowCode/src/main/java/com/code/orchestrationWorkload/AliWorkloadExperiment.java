package com.code.orchestrationWorkload;


import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.fnf.model.v20190315.*;
import com.aliyuncs.profile.DefaultProfile;
import com.code.orchestration.logStreamEntity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class AliWorkloadExperiment {
    // Create DefaultAcsClient
    static DefaultProfile profile = DefaultProfile.getProfile(
            "********",
            "********",
            "********");
    static IAcsClient client = new DefaultAcsClient(profile);

    static String roleArn = "acs:ram::********";
    static String taskResourceArn = "acs:fc:********";


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


    public static void deleteFlow(String flowName) {
        DeleteFlowRequest request = new DeleteFlowRequest();
        request.setName(flowName);
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

            logStreamEntity stream = new logStreamEntity();
            stream.setStreamid(event.getEventId() + "");
            stream.setStreamtype(event.getType());
            stream.setStreamtime(event.getTime());
            stream.setStreamnext(event.getScheduleEventId() + "");
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

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 1000);

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


    public static void getLogMapReduceInfo(String flowName, String execName) {
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

        ArrayList<logStreamEntity> funStartlist = new ArrayList<logStreamEntity>();
        ArrayList<logStreamEntity> funSucceedlist = new ArrayList<logStreamEntity>();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getStreamtype().equals("TaskStarted")) {
                funStartlist.add(list.get(i));
            }
            if (list.get(i).getStreamtype().equals("TaskSucceeded")) {
                funSucceedlist.add(list.get(i));
            }
        }

        long generatedataTime = getTimeInterval(funStartlist.get(0).getStreamtime(), funSucceedlist.get(0).getStreamtime());
        long mapTime = getTimeInterval(funStartlist.get(1).getStreamtime(), funSucceedlist.get(5).getStreamtime());
        long reduceTime = getTimeInterval(funStartlist.get(6).getStreamtime(), funSucceedlist.get(6).getStreamtime());


        funDuration = generatedataTime + mapTime + reduceTime;

        long time = stateTotalTime - funDuration;
        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + mapTime + "\t" + reduceTime;
        System.out.println(dataFormat);

    }


    public static void getLogKMeansInfo(String flowName, String execName) {
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


        ArrayList<logStreamEntity> funStartlist = new ArrayList<logStreamEntity>();

        ArrayList<logStreamEntity> funSucceedlist = new ArrayList<logStreamEntity>();
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getStreamtype().equals("TaskStarted")) {
                funStartlist.add(list.get(i));
            }
            if (list.get(i).getStreamtype().equals("TaskSucceeded")) {
                funSucceedlist.add(list.get(i));
            }
        }

        long generatedataTime = getTimeInterval(funStartlist.get(0).getStreamtime(), funSucceedlist.get(0).getStreamtime());
        long randomTime = getTimeInterval(funStartlist.get(1).getStreamtime(), funSucceedlist.get(1).getStreamtime());
        long kmeansTime = getTimeInterval(funStartlist.get(2).getStreamtime(), funSucceedlist.get(2).getStreamtime());
        long outputTime = getTimeInterval(funStartlist.get(3).getStreamtime(), funSucceedlist.get(3).getStreamtime());


        funDuration = generatedataTime + randomTime + kmeansTime + outputTime;


        long time = stateTotalTime - funDuration;

        String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + randomTime + "\t" + kmeansTime + "\t" + outputTime;

        System.out.println(dataFormat);

    }


    public static void main(String[] args) throws InterruptedException {
//======================================================================


        for (int j = 1; j <= 24; j++) {
            //System.out.print(j+"----");
            startExecution("KMeansExperiment", "RepeatRun" + j, "{ }");
            //TimeUnit.SECONDS.sleep(30);
        }
        for (int j = 1; j <= 24; j++) {
            getLogKMeansInfo("KMeansExperiment", "RepeatRun" + j);
        }


    }

}

