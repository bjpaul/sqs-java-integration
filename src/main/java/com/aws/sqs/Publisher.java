package com.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.util.UUID;

import static com.aws.sqs.SQSFactory.*;

/**
 * Created by bijoypaul on 21/07/17.
 */
public class Publisher {

    private static final String QUEUE_NAME = "myQueue";
    private static final String DL_QUEUE_NAME = "DLQueue";
    private static AmazonSQS sqs = sqsClient();

    public static void main(String[] args){
        createQueue(sqs, QUEUE_NAME);
        createQueue(sqs, DL_QUEUE_NAME);
        configureDeadLetterQueue(sqs, QUEUE_NAME, DL_QUEUE_NAME);
        String queueUrl = getQueueUrl(sqs, QUEUE_NAME);
        sendMessage(queueUrl);
    }

    private static void sendMessage(String queueUrl){

        SendMessageRequest send_msg_request = new SendMessageRequest()
                                            .withQueueUrl(queueUrl);
//                .withDelaySeconds(5);

        Runnable runnable = () -> {
            String msg = "";
            while (true) {
                msg = UUID.randomUUID().toString();
                System.out.println("Publish -> "+msg);
                SendMessageResult result = sqs.sendMessage(send_msg_request.withMessageBody(msg));
                System.out.println(result.getMessageId());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        new Thread(runnable).start();

    }


}
