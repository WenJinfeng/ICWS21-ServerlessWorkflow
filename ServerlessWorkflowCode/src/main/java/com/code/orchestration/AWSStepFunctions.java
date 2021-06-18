package com.code.orchestration;


import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.logs.AWSLogsAsync;
import com.amazonaws.services.logs.AWSLogsAsyncClientBuilder;
import com.amazonaws.services.logs.model.*;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.builder.StateMachine;
import com.amazonaws.services.stepfunctions.builder.states.Branch;
import com.amazonaws.services.stepfunctions.model.*;

import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.services.stepfunctions.builder.StepFunctionBuilder.*;

public class AWSStepFunctions {
    static AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProvider() {
        @Override
        public AWSCredentials getCredentials() {
            AWSCredentials awsCredentials = new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return "******";
                }

                @Override
                public String getAWSSecretKey() {
                    return "******";
                }
            };
            return awsCredentials;
        }

        @Override
        public void refresh() {

        }
    };
    static com.amazonaws.services.stepfunctions.AWSStepFunctions client = AWSStepFunctionsClientBuilder.standard()
            .withRegion("******")//region
            .withCredentials(awsCredentialsProvider)
            .build();

    //create serverless function in AWS Alamda
    static String lambdaArn = "arn:aws:lambda:******";
    //create ASF role about sequence workflow
    static String SequenceRoleArn = "arn:aws:iam::******";
    //create log group about sequence workflow
    static String SequenceLogGroupArn = "arn:aws:logs:******";
    //create ASF role about parallel workflow
    static String ParallelRoleArn = "arn:aws:iam::*******";
    //create log group about parallel workflow
    static String ParallelLogGroupArn = "arn:aws:logs:******";
    //create state machine Arn
    static String StateMachineArn = "arn:aws:states:******";


    static AWSLogsAsync clientlog;
    static AWSLogsAsyncClientBuilder builder;

    static {
        builder = AWSLogsAsyncClientBuilder.standard();
        builder.setCredentials(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                AWSCredentials awsCredentials = new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return "******";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "******";
                    }
                };
                return awsCredentials;
            }

            @Override
            public void refresh() {

            }
        });
        builder.setRegion("******");
        clientlog = builder.build();
    }

    public static AWSLogsAsync getClient() {
        return clientlog;
    }

    //log group name about sequence and parallel workflows
    static String SequenceLogGroup = "/aws/states/******";
    static String ParallelLogGroup = "/aws/states/******";


    //create sequence ASF containing log configuration
    public static void sequenceExperimentByLog(int functionNum, String stateMachineName, String stateMachineDes) {
        StateMachine.Builder stateMachineBuilder = stateMachine().comment(stateMachineDes).startAt("1");
        for (int i = 1; i <= functionNum; i++) {
            stateMachineBuilder.state(String.valueOf(i),
                    taskState().resource(lambdaArn)
                            .transition((i != functionNum) ? next(String.valueOf(i + 1)) : end()));
        }
        StateMachine stateMachine = stateMachineBuilder.build();
        CreateStateMachineResult createResult = client.createStateMachine(
                new CreateStateMachineRequest().withName(stateMachineName)
                        .withLoggingConfiguration(new LoggingConfiguration()
                                .withDestinations(new LogDestination()
                                        .withCloudWatchLogsLogGroup(new CloudWatchLogsLogGroup().withLogGroupArn(SequenceLogGroupArn)))
                                .withIncludeExecutionData(Boolean.TRUE)
                                .withLevel("ALL"))
                        .withRoleArn(SequenceRoleArn)
                        .withDefinition(stateMachine));
        String stateMachineArn = createResult.getStateMachineArn();
        System.out.println(stateMachineArn);

    }


    //create parallel ASF containing log configuration
    public static void parallelExperimentByLog(int functionNum, String stateMachineName, String stateMachineDes) {
        StateMachine.Builder stateMachineBuilder = stateMachine().comment(stateMachineDes).startAt("Parallel");
        Branch.Builder[] branchBuillders = new Branch.Builder[functionNum];
        for (int i = 0; i < functionNum; i++) {
            branchBuillders[i] = branch()
                    .startAt(String.valueOf(i + 1))
                    .state(String.valueOf(i + 1),
                            taskState()
                                    .resource(lambdaArn).transition(end()));
        }
        stateMachineBuilder.state("Parallel",
                parallelState().branches(branchBuillders)
                        .transition(end()));
        StateMachine stateMachine = stateMachineBuilder.build();
        CreateStateMachineResult createResult = client.createStateMachine(
                new CreateStateMachineRequest().withName(stateMachineName)
                        .withLoggingConfiguration(new LoggingConfiguration()
                                .withDestinations(new LogDestination()
                                        .withCloudWatchLogsLogGroup(new CloudWatchLogsLogGroup().withLogGroupArn(ParallelLogGroupArn)))
                                .withIncludeExecutionData(Boolean.TRUE)
                                .withLevel("ALL"))
                        .withRoleArn(ParallelRoleArn)
                        .withDefinition(stateMachine));
        String stateMachineArn = createResult.getStateMachineArn();
        System.out.println(stateMachineArn);
    }

    //execute ASF
    public static void exeMachine(String machineArnname, String inputContent) {

        StartExecutionRequest request = new StartExecutionRequest();

        request.setStateMachineArn(StateMachineArn + machineArnname);
        request.setInput(inputContent);
        StartExecutionResult result = client.startExecution(request);

        System.out.println(result);
    }

    //get log information in sequence workflows
    public static void getLogSequenceInfoByLogStreamName(String logstream) {

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(SequenceLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);

        List<LogStream> logstreamList = describeLogStreamsResult.getLogStreams();

        System.out.println(logstream);
        GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
                .withLogGroupName(SequenceLogGroup)
                .withLogStreamName(logstream).withLimit(1000);
        GetLogEventsResult result = clientlog.getLogEvents(getLogEventsRequest);
        List<OutputLogEvent> outputLogEvent = result.getEvents();

        ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();

        for (OutputLogEvent log : outputLogEvent) {
            String tmpStr = log.getMessage();
            JSONObject json_test = JSONObject.parseObject(tmpStr);
            System.out.println(json_test.get("id") + "-" + json_test.get("type") + "-" + json_test.get("event_timestamp") + "-" + json_test.get("previous_event_id"));
            logStreamEntity stream = new logStreamEntity();
            stream.setStreamid((String) json_test.get("id"));
            stream.setStreamtype((String) json_test.get("type"));
            stream.setStreamtime((String) json_test.get("event_timestamp"));
            stream.setStreamnext((String) json_test.get("previous_event_id"));
            list.add(stream);

        }

        long stateStartTime = Long.parseLong((list.get(0).getStreamtime()));
        long stateEndTime = Long.parseLong(list.get(list.size() - 1).getStreamtime());
        long stateTotalTime = stateEndTime - stateStartTime;

        long funDuration = 0;
        for (logStreamEntity stream : list) {
            if (stream.getStreamtype().equals("LambdaFunctionSucceeded")) {
                String nextStr = stream.getStreamnext();
                for (int i = 0; i < list.size(); i++) {
                    if (list.get(i).getStreamid().equals(nextStr)) {
                        long endtime = Long.parseLong(stream.getStreamtime());
                        long starttime = Long.parseLong(list.get(i).getStreamtime());
                        funDuration += (endtime - starttime);
                    }
                }
            }
        }

        long time = stateTotalTime - funDuration;


        System.out.println(list.size());
        String loginfo = "stateTotalTime:" + stateTotalTime + ",funDuration:" + funDuration + ",transitionTime:" + time;

        System.out.println(loginfo);

    }


    //get log information about parallel workflows
    public static void getLogParallelInfoByLogStream(String logstream) {
        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(ParallelLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);

        System.out.println(logstream);
        GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
                .withLogGroupName(ParallelLogGroup)
                .withLogStreamName(logstream).withLimit(1000);

        GetLogEventsResult result = clientlog.getLogEvents(getLogEventsRequest);
        List<OutputLogEvent> outputLogEvent = result.getEvents();

        ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();

        for (OutputLogEvent log : outputLogEvent) {
            String tmpStr = log.getMessage();
            JSONObject json_test = JSONObject.parseObject(tmpStr);
            System.out.println(json_test.get("id") + "-" + json_test.get("type") + "-" + json_test.get("event_timestamp") + "-" + json_test.get("previous_event_id"));
            logStreamEntity stream = new logStreamEntity();
            stream.setStreamid((String) json_test.get("id"));
            stream.setStreamtype((String) json_test.get("type"));
            stream.setStreamtime((String) json_test.get("event_timestamp"));
            stream.setStreamnext((String) json_test.get("previous_event_id"));
            list.add(stream);

        }

        long stateStartTime = Long.parseLong((list.get(0).getStreamtime()));
        long stateEndTime = Long.parseLong(list.get(list.size() - 1).getStreamtime());
        long stateTotalTime = stateEndTime - stateStartTime;

        long funDuration = 0;

        long starttime = 0;
        long endtime = 0;

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getStreamtype().equals("LambdaFunctionStarted")) {
                starttime = Long.parseLong(list.get(i).getStreamtime());
                break;
            }
        }

        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).getStreamtype().equals("LambdaFunctionSucceeded")) {
                endtime = Long.parseLong(list.get(i).getStreamtime());
                break;
            }
        }

        funDuration = endtime - starttime;

        long time = stateTotalTime - funDuration;


        System.out.println(list.size());
        String loginfo = "stateTotalTime:" + stateTotalTime + ",funDuration:" + funDuration + ",transitionTime:" + time;

        System.out.println(loginfo);
    }


    public static void main(String[] args) throws InterruptedException {
        //sequence workflow experiments
        int i = 80;
        sequenceExperimentByLog(i, "sequenceNoPayload" + i, "experiment");
        exeMachine("sequenceNoPayload" + i, "{ }");
        getLogSequenceInfoByLogStreamName("******");
        //=====================================================================
        //parallel workflow experiments
        //int i = 80;
        parallelExperimentByLog(i, "parallelNoPayload" + i, "experiment");
        exeMachine("parallelNoPayload" + i, "{ }");
        getLogParallelInfoByLogStream("******");


    }

}