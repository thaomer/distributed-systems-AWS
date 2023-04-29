package org.example;

import org.example.AWS.SQS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

import static org.example.Manager.*;

public class WorkerRequestHandler implements Runnable{
    String workerToManagerQueue;

    public  WorkerRequestHandler(String workerToManagerQueue) {
        this.workerToManagerQueue = workerToManagerQueue;
    }

    @Override
    public void run() {

        while((localAppsResultMap.size() > 0) || !terminated.get()) {
            List<Message> messagesFromWorker = SQS.receiveMessage(sqs, workerToManagerQueue);

            if (!messagesFromWorker.isEmpty()) {
                System.out.println("in 11");
                Message message = messagesFromWorker.get(0);
                String[] workerResult = message.body().split("\\n");
                SQS.deleteMessage(sqs, workerToManagerQueue, message);
                numSizeQueue.getAndDecrement();


                //resultKey is the mangerToLocalApp url address which represent the specific localApp key in the hashMap
                String resultKey = workerResult[0];
                String resultBody = message.body().substring(resultKey.length() + 1);

                //get the current OCR deciphered string list and add the new string
                OCRResult OCRResultList = localAppsResultMap.get(resultKey);
                OCRResultList.OCRStringList.add(resultBody);

                //if we finished to decipher for this queue create the summary file and after that remove it from the hashmap
                if (OCRResultList.isFull()) {
                    createAndSendSummaryFile(OCRResultList.OCRStringList, resultKey);
                    localAppsResultMap.remove(resultKey);
                }
            }
        }
    }
}
