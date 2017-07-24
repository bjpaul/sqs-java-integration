package com.aws.sqs;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

import static com.aws.sqs.SQSFactory.*;
/**
 * Created by bijoypaul on 21/07/17.
 */
public class Subscriber {
    private static final String QUEUE_NAME = "myQueue";
    private static AmazonSQS sqs = sqsClient();

    public static void main(String[] args){
        createQueue(sqs, QUEUE_NAME);
        String queueUrl = getQueueUrl(sqs, QUEUE_NAME);
        receiveMessage(queueUrl);
    }

    private static void receiveMessage(String queueUrl){
        // Enable long polling on a message receipt
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);

        Runnable runnable = () -> {
            while (true){
                // receive messages from the queue with long polling
                List<Message> messages = sqs.receiveMessage(receive_request).getMessages();
                // receive messages from the queue
//                List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();

                for (Message message : messages) {
                    try {
                        processMessage(queueUrl, message);
                        deleteMessage(queueUrl, message);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                System.out.println("***********************");
                /*try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        };

        new Thread(runnable).start();

    }

    // Change the visibility timeout for a single message
    private static void changeMessageVisibilitySingle(
            String queue_url, Message message, int timeout)
    {
        // Get the receipt handle for the first message in the queue.
        String receipt = message.getReceiptHandle();

        sqs.changeMessageVisibility(queue_url, receipt, timeout);
    }

    private static void processMessage(String queueUrl, Message message){
        changeMessageVisibilitySingle(queueUrl, message, 180);
        System.out.println("  Message");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
    }

    private static void deleteMessage(String queueUrl, Message message){
        sqs.deleteMessage(queueUrl, message.getReceiptHandle());
    }
}
