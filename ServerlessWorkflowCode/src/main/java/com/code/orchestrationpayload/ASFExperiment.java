package com.code.orchestrationpayload;


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
import com.code.orchestration.logStreamEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.stepfunctions.builder.StepFunctionBuilder.*;

/**
 * @author : Cynthia
 * @date :
 */
public class ASFExperiment {
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
                    return "*******";
                }
            };
            return awsCredentials;
        }

        @Override
        public void refresh() {

        }
    };
    static com.amazonaws.services.stepfunctions.AWSStepFunctions client = AWSStepFunctionsClientBuilder.standard()
            .withRegion("******")
            .withCredentials(awsCredentialsProvider)
            .build();


    static String lambdaArn = "arn:aws:lambda:*****";

    static String SequenceRoleArn = "arn:aws:iam::********";
    static String SequenceLogGroupArn = "arn:aws:logs:********";
    static String ParallelRoleArn = "arn:aws:iam::********";
    static String ParallelLogGroupArn = "arn:aws:logs:********";

    static String StateMachineArn = "arn:aws:states:********";

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
                        return "********";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "********";
                    }
                };
                return awsCredentials;
            }

            @Override
            public void refresh() {

            }
        });
        builder.setRegion("********");
        clientlog = builder.build();
    }

    public static AWSLogsAsync getClient() {
        return clientlog;
    }

    static String SequenceLogGroup = "/aws/states/********";
    static String ParallelLogGroup = "/aws/states/********";


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

    public static void exeMachine(String machineArnname, String runname, String inputContent) {

        StartExecutionRequest request = new StartExecutionRequest();

        request.setStateMachineArn(StateMachineArn + machineArnname);
        request.setInput(inputContent);
        request.setName(runname);
        StartExecutionResult result = client.startExecution(request);

        System.out.println(result);
    }


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

    public static void testSequenceLog() {

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(SequenceLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);


        List<LogStream> logstreamList = describeLogStreamsResult.getLogStreams();

        for (int k = logstreamList.size() - 1; k >= logstreamList.size() - 24; k--) {
            System.out.println(25 - k);
            System.out.println(logstreamList.get(k).getLogStreamName());
        }
    }

    public static void getMultiLogSequenceInfo(int logNum) {

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(SequenceLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);

        List<LogStream> logstreamList = describeLogStreamsResult.getLogStreams();

        for (int k = logstreamList.size() - 1; k >= logstreamList.size() - logNum; k--) {

            GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
                    .withLogGroupName(SequenceLogGroup)
                    .withLogStreamName(logstreamList.get(k).getLogStreamName()).withLimit(1000);
            GetLogEventsResult result = clientlog.getLogEvents(getLogEventsRequest);
            List<OutputLogEvent> outputLogEvent = result.getEvents();

            ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();

            for (OutputLogEvent log : outputLogEvent) {
                String tmpStr = log.getMessage();
                JSONObject json_test = JSONObject.parseObject(tmpStr);
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

            String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time;
            System.out.println(dataFormat);
        }
    }

    public static void getMultiLogParallelInfo(int logNum) {
        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(ParallelLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);

        List<LogStream> logstreamList = describeLogStreamsResult.getLogStreams();
        for (int k = logstreamList.size() - 1; k >= logstreamList.size() - logNum; k--) {

            GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
                    .withLogGroupName(ParallelLogGroup)
                    .withLogStreamName(logstreamList.get(k).getLogStreamName()).withLimit(1000);

            GetLogEventsResult result = clientlog.getLogEvents(getLogEventsRequest);
            List<OutputLogEvent> outputLogEvent = result.getEvents();

            ArrayList<logStreamEntity> list = new ArrayList<logStreamEntity>();

            for (OutputLogEvent log : outputLogEvent) {
                String tmpStr = log.getMessage();
                JSONObject json_test = JSONObject.parseObject(tmpStr);
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

            String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + (stateTotalTime - 1000);
            System.out.println(dataFormat);
        }
    }

    public static void testParallelLog() {

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(ParallelLogGroup);
        DescribeLogStreamsResult describeLogStreamsResult = clientlog.describeLogStreams(describeLogStreamsRequest);

        List<LogStream> logstreamList = describeLogStreamsResult.getLogStreams();

        for (int k = logstreamList.size() - 1; k >= logstreamList.size() - 22; k--) {
            System.out.println(23 - k);
            System.out.println(logstreamList.get(k).getLogStreamName());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //generate
        String str = "{\"name\" : \"%s\"}";
        String repeatchar = "a";
        String repeatstring = new String(new char[262131]).replace("\0", repeatchar);

        String payload = String.format(str, repeatstring);

        //sequence workflows with 5 functions, which have payload
        sequenceExperimentByLog(5, "sequence5Payload", "experiment");
        int k = 16;
        for (int i = 1; i <= 24; i++) {
            exeMachine("sequence5Payload", "NewRun2_" + k + "_" + i, payload);
            TimeUnit.SECONDS.sleep(25);
        }
        testSequenceLog();
        getMultiLogSequenceInfo(24);


        //parallel workflows with 5 functions, which have payload
        parallelExperimentByLog(5, "parallel5Payload", "experiment");
        //int k=15;
        for (int i = 1; i <= 24; i++) {
            exeMachine("parallel5Payload", "Run2_" + k + "_" + i, payload);
            TimeUnit.SECONDS.sleep(70);
        }
        testParallelLog();
        getMultiLogParallelInfo(22);

    }

}


