package org.example;

import org.example.AWS.SQS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

import static org.example.Manager.*;

public class LocalAppRequestHandler implements Runnable{
    String localAppsToManagerQueue;
    String managerToWorkerQueue;
    String bucketName;

    public LocalAppRequestHandler(String localAppsToManagerQueue, String managerToWorkerQueue, String bucketName){
        this.localAppsToManagerQueue = localAppsToManagerQueue;
        this.managerToWorkerQueue = managerToWorkerQueue;
        this.bucketName = bucketName;
    }

    @Override
    public void run() {

        while ((localAppsResultMap.size() > 0) || !terminated.get()) {
            List<Message> messagesFromLocalApp = SQS.receiveMessage(sqs,localAppsToManagerQueue);//maybe pdont create List everytime?

            if(!messagesFromLocalApp.isEmpty()) {// has jobs
                System.out.println("message is not empty, get the message from queue");
                //get message from localApp and split it
                Message message = messagesFromLocalApp.get(0);
                String [] localAppMessage = splitLocalAppMessage(message.body());
                SQS.deleteMessage(sqs, localAppsToManagerQueue,message);

                //check message validation
                if (localAppMessage.length < 1) {
                    System.out.println("Error, local app message is empty");
                }

                boolean isTermination = false;
                String localMessageType = localAppMessage[0];
                String fileKey;
                String localAppQueueUrl;
                System.out.println("dis is local message type: "+localMessageType);
                if (localMessageType.equals("terminate")) {
                    isTermination = true;
                    fileKey = localAppMessage[1];
                    localAppQueueUrl = localAppMessage[2];
                } else {
                    fileKey = localAppMessage[0];
                    localAppQueueUrl = localAppMessage[1];
                }
                //if no termination so we take the data from the message and pass it to handle input file method
                handleInputFile(fileKey, localAppQueueUrl, managerToWorkerQueue);
                System.out.println("is termination"+localMessageType);
                if (isTermination) {
                        handleTermination();
                }
            }
        }

    }

    public static String[] splitLocalAppMessage(String localAppMessage){
        return localAppMessage.split("\\n");
    }


}
