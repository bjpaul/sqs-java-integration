package com.aws.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

/**
 * Created by bijoypaul on 21/07/17.
 */
public class SQSFactory {

    public static AmazonSQS sqsClient(){
         /*
         * [default] profile reading from the credentials file located at
         * (~/.aws/credentials).
         */

        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard();
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(5);
        clientBuilder.setClientConfiguration(clientConfiguration);
        AmazonSQS amazonSQS = clientBuilder
                                .withRegion("us-east-1")
                                .build();
        return amazonSQS;
    }

    public static void createQueue(AmazonSQS sqs, String QUEUE_NAME){
        CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME);
//                .addAttributesEntry("DelaySeconds", "60")
//                .addAttributesEntry("MessageRetentionPeriod", "86400");

        try {
            sqs.createQueue(create_request);

        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
    }

    public static void listAllQueues(AmazonSQS sqs){
        ListQueuesResult lq_result = sqs.listQueues();
        System.out.println("Your SQS Queue URLs:");
        for (String url : lq_result.getQueueUrls()) {
            System.out.println(url);
        }
    }

    public static void listQueuesWithNamePrefix(AmazonSQS sqs, String namePrefix){
        ListQueuesResult lq_result = sqs.listQueues(new ListQueuesRequest(namePrefix));
        System.out.println("Queue URLs with prefix: " + namePrefix);
        for (String url : lq_result.getQueueUrls()) {
            System.out.println(url);
        }
    }

    public static String getQueueUrl(AmazonSQS sqs, String queueName){

        try {
            System.out.println("Queue URLs with name: " + queueName);
            String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
            System.out.println(queueUrl);
            return queueUrl;

        } catch (QueueDoesNotExistException e) {
            System.out.println(queueName+" does not exist");
            return "";
        }
    }

    public static void deleteQueue(AmazonSQS sqs, String queueUrl){
        try {
            System.out.println("Deleting queue wit URL: " + queueUrl);
            sqs.deleteQueue(queueUrl);

        } catch (QueueDoesNotExistException e) {
            System.out.println(queueUrl+" does not exist");
        }
    }

    public static void configureDeadLetterQueue(AmazonSQS sqs, String src_queue_name, String dl_queue_name){
        // Get dead-letter queue ARN
        String dl_queue_url = sqs.getQueueUrl(dl_queue_name)
                .getQueueUrl();

        GetQueueAttributesResult queue_attrs = sqs.getQueueAttributes(
                new GetQueueAttributesRequest(dl_queue_url)
                        .withAttributeNames("QueueArn"));

        String dl_queue_arn = queue_attrs.getAttributes().get("QueueArn");

        // Set dead letter queue with redrive policy on source queue.
        String src_queue_url = sqs.getQueueUrl(src_queue_name)
                .getQueueUrl();

        SetQueueAttributesRequest request = new SetQueueAttributesRequest()
                .withQueueUrl(src_queue_url)
                .addAttributesEntry("RedrivePolicy",
                        "{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\""
                                + dl_queue_arn + "\"}");

        sqs.setQueueAttributes(request);
    }
}
