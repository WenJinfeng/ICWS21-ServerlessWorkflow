package com.code.orchestrationWorkload;


import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.logs.AWSLogsAsync;
import com.amazonaws.services.logs.AWSLogsAsyncClientBuilder;
import com.amazonaws.services.logs.model.*;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.DeleteStateMachineRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.code.orchestration.logStreamEntity;

import java.util.ArrayList;
import java.util.List;

//https://github.com/aws/aws-sdk-java/tree/master/aws-java-sdk-stepfunctions


public class ASFWorkloadExperiment {
    static AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProvider() {
        @Override
        public AWSCredentials getCredentials() {
            AWSCredentials awsCredentials = new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return "*******";
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
            .withRegion("*******")
            .withCredentials(awsCredentialsProvider)
            .build();

    static String lambdaArn = "arn:aws:lambda:*******";
    static String SequenceRoleArn = "arn:aws:iam::*******";
    static String SequenceLogGroupArn = "arn:aws:*******";
    static String ParallelRoleArn = "arn:aws:iam::*******";
    static String ParallelLogGroupArn = "arn:aws:*******";

    static String StateMachineArn = "arn:aws:*******";

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
                        return "*******";
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
        });
        builder.setRegion("*******");
        clientlog = builder.build();
    }

    public static AWSLogsAsync getClient() {
        return clientlog;
    }

    static String SequenceLogGroup = "/aws/states/*******";
    static String ParallelLogGroup = "/aws/states/*******";


    public static void exeMachine(String machineArnname, String runname, String inputContent) {

        StartExecutionRequest request = new StartExecutionRequest();

        request.setStateMachineArn(StateMachineArn + machineArnname);
        request.setInput(inputContent);
        request.setName(runname);
        StartExecutionResult result = client.startExecution(request);

        System.out.println(result);
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

    public static void deleteMachine(String machineArnname) {
        client.deleteStateMachine(new DeleteStateMachineRequest()
                .withStateMachineArn(StateMachineArn + machineArnname));
        System.out.println("delete" + machineArnname);
    }

    public static void getMultiLogMapReduceInfo(int logNum) {
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
                //System.out.println(json_test.get("id") + "-" + json_test.get("type") + "-" + json_test.get("event_timestamp") + "-" + json_test.get("previous_event_id"));
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
            ArrayList<logStreamEntity> funStartlist = new ArrayList<logStreamEntity>();

            ArrayList<logStreamEntity> funSucceedlist = new ArrayList<logStreamEntity>();

            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getStreamtype().equals("LambdaFunctionStarted")) {
                    funStartlist.add(list.get(i));
                }
                if (list.get(i).getStreamtype().equals("LambdaFunctionSucceeded")) {
                    funSucceedlist.add(list.get(i));
                }
            }

            long generatedataTime = Long.parseLong(funSucceedlist.get(0).getStreamtime()) - Long.parseLong(funStartlist.get(0).getStreamtime());
            long mapTime = Long.parseLong(funSucceedlist.get(5).getStreamtime()) - Long.parseLong(funStartlist.get(1).getStreamtime());
            long reduceTime = Long.parseLong(funSucceedlist.get(6).getStreamtime()) - Long.parseLong(funStartlist.get(6).getStreamtime());


            funDuration = generatedataTime + mapTime + reduceTime;
            long time = stateTotalTime - funDuration;
            String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + mapTime + "\t" + reduceTime;
            System.out.println(dataFormat);
        }
    }

    public static void getMultiLogKmeansInfo(int logNum) {
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


            ArrayList<logStreamEntity> funStartlist = new ArrayList<logStreamEntity>();

            ArrayList<logStreamEntity> funSucceedlist = new ArrayList<logStreamEntity>();

            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getStreamtype().equals("LambdaFunctionStarted")) {
                    funStartlist.add(list.get(i));
                }
                if (list.get(i).getStreamtype().equals("LambdaFunctionSucceeded")) {
                    funSucceedlist.add(list.get(i));
                }
            }

            long generatedataTime = Long.parseLong(funSucceedlist.get(0).getStreamtime()) - Long.parseLong(funStartlist.get(0).getStreamtime());
            long randomTime = Long.parseLong(funSucceedlist.get(1).getStreamtime()) - Long.parseLong(funStartlist.get(1).getStreamtime());
            long kmeansTime = Long.parseLong(funSucceedlist.get(2).getStreamtime()) - Long.parseLong(funStartlist.get(2).getStreamtime());
            long outputTime = Long.parseLong(funSucceedlist.get(3).getStreamtime()) - Long.parseLong(funStartlist.get(3).getStreamtime());


            funDuration = generatedataTime + randomTime + kmeansTime + outputTime;
            long time = stateTotalTime - funDuration;

            String dataFormat = stateTotalTime + "\t" + funDuration + "\t" + time + "\t" + generatedataTime + "\t" + randomTime + "\t" + kmeansTime + "\t" + outputTime;

            System.out.println(dataFormat);
        }
    }

    public static void main(String[] args) throws InterruptedException {


        for (int j = 1; j <= 22; j++) {
            exeMachine("MapReduceExperiment", "run" + j, "{ }");
            //TimeUnit.SECONDS.sleep(20);
        }
        testSequenceLog();
        getMultiLogMapReduceInfo(22);


        for (int j = 1; j <= 24; j++) {
            exeMachine("KMeansExperiment", "RepeatRun" + j, "{ }");
            //TimeUnit.SECONDS.sleep(30);
        }

        testSequenceLog();
        getMultiLogKmeansInfo(24);
    }
}
