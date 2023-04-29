package org.example;


import org.example.AWS.ECC;
import org.example.AWS.S3;
import org.example.AWS.SQS;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.AWS.SQS.*;


public class Manager {
    //sqs service
    static S3Client s3;
    static SqsClient sqs;
    static Ec2Client ec2;
    static final String amiId = "ami-061b8ce3b07516cfa";
    static String bucketName = "fwnwnfwonowfn";

    //workers instances
    static int numOfTasksPerWorker; // init as large number for now so we won't open many workers
    static final int maxWorkers = 10;
    public static AtomicInteger numSizeQueue = new AtomicInteger(0);//TODO change atomic to sync? manager to worker queue size
    static Integer numOfWorkers = 0;

    //each local application queue usl is the key and OCR result conatins the deciphered images
    public static HashMap<String, OCRResult> localAppsResultMap = new HashMap();
    //key is the local application that sent input file and value is the String to return so we can make file


    public static final int numOfManagerWorkerThreads = 3;
    public static AtomicBoolean terminated = new AtomicBoolean(false);




    public static void main(String[] args){
        makeAWSClients();
        String localAppsToManagerQueue = createQueue(sqs, "localAppsToManagerQueue");
        String managerToWorkerQueue = createQueue(sqs, "managerToWorkerQueue");
        String workerToManagerQueue = createQueue(sqs, "workerToManagerQueue");

        System.out.println("Finished to create queues, start receiving messages");

        numOfTasksPerWorker = Integer.parseInt(args[0]);

        LocalAppRequestHandler localAppHandler = new LocalAppRequestHandler(localAppsToManagerQueue, managerToWorkerQueue, bucketName);
        WorkerRequestHandler workerHandler = new WorkerRequestHandler(workerToManagerQueue);
        CheckWorkers checkWorkers = new CheckWorkers();


        //use executor
        ExecutorService executorService = Executors.newFixedThreadPool(2 + numOfManagerWorkerThreads);
        executorService.execute(localAppHandler);
        executorService.execute(checkWorkers);
        for(int i = 0; i < numOfManagerWorkerThreads; i++) {
            executorService.execute(workerHandler);
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
        }
        
    }


    public static void handleTermination(){
        System.out.println("i am hadeling termination");
        terminated.set(true);
        System.out.println("localAppsResultMap:"+localAppsResultMap.size());
        while (localAppsResultMap.size() > 0) {
            try {
                TimeUnit.SECONDS.sleep(2);
            }
            catch (Exception e)
            {
                System.out.println("### dont wake me up");
            }
        }
        try {
            TimeUnit.SECONDS.sleep(30);
        }
        catch (Exception e)
        {
            System.out.println("### dont wake me up");
        }
        System.out.println("localAppsResultMap:"+localAppsResultMap.size());
        System.out.println("i have passed the while");
        ECC.terminateWorkers(ec2);
        SQS.deleteSQSQueue(sqs, "managerToWorkerQueue");
        SQS.deleteSQSQueue(sqs, "workerToManagerQueue");
        SQS.deleteSQSQueue(sqs, "localAppsToManagerQueue");
        ECC.terminateManager(ec2);

    }


    public static void
    handleInputFile(String inputFileKey, String localAppQueueUrl, String managerToWorkerQueue) {

        String inputFileContent = S3.getS3Object(s3, bucketName, inputFileKey);//assume message.body is key
        String [] imagesUrlsArray = inputFileContent.split("\\n");

        S3.deleteS3Object(s3, bucketName, inputFileKey);
        //maybe make a second function and return urls by the first
        OCRResult ocrResultList = new OCRResult(imagesUrlsArray.length);//maybe do like a for loop with a queue add
        localAppsResultMap.put(localAppQueueUrl, ocrResultList);
        System.out.println("local app key in handle input is:\n" + localAppQueueUrl);
        System.out.println(localAppsResultMap.get(localAppQueueUrl).isFull());

        //generate new message and append id of localAppQueueUrl which is also the key in the hashmap and send the message to worker queue
        for (int i = 0; i < imagesUrlsArray.length; i++) {
            imagesUrlsArray[i] += "\n";
            imagesUrlsArray[i] += localAppQueueUrl;
            SQS.sendMessage(sqs, managerToWorkerQueue, imagesUrlsArray[i]);
        }
        numSizeQueue.getAndAdd(imagesUrlsArray.length);



    }



    //TODO shay - make sure that summary file is unique name
    public static void createAndSendSummaryFile(ConcurrentLinkedQueue<String> summaryList, String managerToLocalAppQueue) {
        try {
            //create new file to enter the data to
            File summaryFile = new File("summaryFile" + System.nanoTime() + ".txt");

            if (summaryFile.createNewFile()) {
                System.out.println("Summary file created: " + summaryFile.getName());

                FileWriter writeToFile = new FileWriter(summaryFile);
                for (String imageOcr : summaryList) {
                    writeToFile.write(imageOcr + "\n");
                }
                writeToFile.close();

                //upload file to s3
                String fileIdInS3 = S3.putS3Object(s3, bucketName, summaryFile.getName());
                SQS.sendMessage(sqs, managerToLocalAppQueue, fileIdInS3);

                if (summaryFile.delete()) {
                    System.out.println("Deleted the file from local repository: " + summaryFile.getName());
                } else {
                    System.out.println("Failed to delete the file.");
                }

            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void makeAWSClients() {
        Region region = Region.US_EAST_1;

        //s3
        s3 = S3Client.builder()
                .region(region)
                .build();

        S3.createBucket(s3, bucketName);

        //ec2
        ec2 = Ec2Client.builder()
                .region(region)
                .build();

        //sqs
        sqs = SqsClient.builder()
                .region(region)
                .build();

    }

}