import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.Iterator;

public class ManagerThread extends Thread {
    private static AWS aws;
    private Map<String, LocalAppInfo> localAppsInfo;
    private AtomicBoolean shouldTerminate;
    private Queue<LocalAppInfo> numOfWorkersQueue;
    private AtomicBoolean isTerminated;
    private Object lock;

    public ManagerThread(AWS aws, Map<String, LocalAppInfo> localAppsInfo, AtomicBoolean shouldTerminate, Queue<LocalAppInfo> numOfWorkersQueue, AtomicBoolean isTerminated, Object lock) {
        this.aws = aws;
        this.localAppsInfo = localAppsInfo;
        this.shouldTerminate = shouldTerminate;
        this.numOfWorkersQueue = numOfWorkersQueue;
        this.isTerminated = isTerminated;
        this.lock = lock;
    }

    public void run() {
        while (true) {
            String managerUrlSQS = aws.getSQSUrl(aws.managerSQSName);
            Message currMessage = aws.getNewMessage(managerUrlSQS);
            if (currMessage != null) {
                System.out.println("[DEBUG] manager get a massage" + currMessage.body());
                aws.deleteMessageFromSQS(managerUrlSQS, currMessage);
                String[] splitMsg = currMessage.body().split(aws.delimiter);
                System.out.println("[DEBUG] massage type " + splitMsg[0]);
                if (splitMsg[0].equals("NewTask")) {
                    System.out.println("[DEBUG] The message is NewTask");
                    if (!shouldTerminate.get()) {
                        try {
                            handleNewTask(splitMsg);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else
                        System.out.println("[DEBUG] can't get newTask already got terminate message, will open new manager");
                } else if (splitMsg[0].equals("DoneOCR")) {
                    System.out.println("[DEBUG] The message is DoneOCR");
                    handleDoneOCR(splitMsg);
                } else if (splitMsg[0].equals("Terminate")) {
                    shouldTerminate.set(true);
                }
            }
            synchronized (lock) {
                if (shouldTerminate.get() && localAppsInfo.isEmpty() && !isTerminated.get()) {
                    isTerminated.set(true);
                    handleTerminate();
                    System.out.println("[DEBUG] Terminate");
                    break;
                } else if (isTerminated.get())
                    break;
            }
        }
    }


    private void handleNewTask(String[] splitMsg) throws IOException {
        System.out.println("[DEBUG] handleNewTask");
        String bucketName = splitMsg[1];
        String localAppId = splitMsg[2];
        int tasksPerWorker = Integer.valueOf(splitMsg[3]);
        int numOfTasks = handleFile(bucketName, localAppId);
        int numOfWorkers = Math.min((int) Math.ceil((double) numOfTasks / tasksPerWorker), Manager.MAX_INSTANCES);
        LocalAppInfo currLocalAppInfo = new LocalAppInfo(numOfTasks, numOfWorkers);
        localAppsInfo.put(localAppId, currLocalAppInfo);
        numOfWorkersQueue.add(currLocalAppInfo);
        createNewWorkers(numOfWorkers);
    }

    private void createNewWorkers(int requestedNumOfWorkers) {
        System.out.println("[DEBUG] create " + requestedNumOfWorkers + " workers");
        int currentNumOfWorker = aws.numOfInstance("worker");
        String workerScript = "#! /bin/bash\n" +
                "echo Worker jar running\n" +
                "mkdir WorkerFiles\n" +
                "aws s3 cp " + "s3://" + aws.bucketName + "/" + aws.workerJarKey + " ./WorkerFiles/" + aws.workerJarName + "\n" +
                "java -jar /WorkerFiles/" + aws.workerJarName + "\n";
        int numOfWorkersToCreate = requestedNumOfWorkers - currentNumOfWorker;
        aws.createInstance(workerScript, "worker", numOfWorkersToCreate);
    }

    private void handleDoneOCR(String[] splitMsg) {
        System.out.println("[DEBUG] Function: handleDoneOCR. Args: " + Arrays.toString(splitMsg));
        String localAppId = splitMsg[1];
        String url = splitMsg[2];
        String result = splitMsg[3];
        String output = "img src = " + url + "\t" + result;
        try {
            localAppsInfo.get(localAppId).getTasksResult().putIfAbsent(url, output);
            int numOfLeftTask = localAppsInfo.get(localAppId).getNumOfTaskLeft().decrementAndGet();
            System.out.println("numOfLeftTask: " + numOfLeftTask + "for localAppId " + localAppId);
            if (numOfLeftTask == 0) {
                creatOutputAndSendDoneMessage(localAppId);
                updateNumOfWorkers(localAppId);
                localAppsInfo.remove(localAppId);
            }
        } catch (Exception e) {
            System.out.println("[DEBUG] Already done handling localApp id: " + localAppId);
        }
    }


    private void updateNumOfWorkers(String localAppId) {
        System.out.println("[DEBUG] updateNumOfWorkers");
        LocalAppInfo localApp = localAppsInfo.get(localAppId);
        synchronized (numOfWorkersQueue) {
            if (numOfWorkersQueue.peek() == localApp) {
                int numOfWorkersToRemove = numOfWorkersQueue.poll().getNumOfworkers();
                if (numOfWorkersQueue.peek() != null)
                    numOfWorkersToRemove -= numOfWorkersQueue.peek().getNumOfworkers();
                shutdownWorkers(numOfWorkersToRemove);
            } else {
                numOfWorkersQueue.remove(localApp);
            }
        }
    }

    private void shutdownWorkers(int numOfWorkersToRemove) {
        System.out.println("[DEBUG] shutdownWorkers");
        for (int i = 0; i < numOfWorkersToRemove; i++) {
            aws.sendMessage(aws.getSQSUrl(aws.workerSQSName), "Terminate");
            System.out.println("[DEBUG] send Terminate to worker " + i);
        }
    }

    private void creatOutputAndSendDoneMessage(String localAppId) {
        System.out.println("[DEBUG] creatOutputAndSendDoneMessage");
        Map currLocalAppMapResult = localAppsInfo.get(localAppId).getTasksResult();
        Iterator iter = currLocalAppMapResult.keySet().iterator();
        String text = "";
        while (iter.hasNext()) {
            text += currLocalAppMapResult.get(iter.next()) + "###";
        }

        System.out.println("[DEBUG] the text of output is:" + text);
        String outputPath = "./" + localAppId + ".txt";
        String keyName = "outputs/" + localAppId + ".txt";
        PrintWriter fileTextOutput = null;
        try {
            fileTextOutput = new PrintWriter(outputPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fileTextOutput.print(text);
        fileTextOutput.close();
        sendDoneMessage(localAppId, outputPath, keyName);
    }

    private static String createDoneMessage(String localAppId, String keyName) {
        System.out.println("[DEBUG] createDoneMessage");
        String message = aws.bucketName +
                aws.delimiter +
                keyName;
        return message;
    }

    private static void sendDoneMessage(String localAppId, String outputPath, String keyName) {
        System.out.println("[DEBUG] sendDoneMessage");
        aws.uploadFileToS3(aws.bucketName, keyName, outputPath);
        aws.sendMessage(aws.getSQSUrl(aws.localAppSQSName + localAppId), createDoneMessage(localAppId, keyName));
    }

    private void handleTerminate() {
        System.out.println("[DEBUG] handleTerminate");
        try {
            while (aws.checkIfInstanceExist("worker")) {
                aws.sendMessage(aws.getSQSUrl(aws.workerSQSName), "Terminate");
                sleep(1000);
            }
        } catch (Exception e) {
        }
        aws.deleteSQS(aws.workerSQSName);
        aws.deleteBucket(aws.bucketName);
        aws.deleteSQS(aws.managerSQSName);
        aws.shutdownInstance();
    }

    private int handleFile(String bucketName, String localAppId) throws IOException {
        System.out.println("[DEBUG] handleFile");
        String fileContent = aws.downloadFileFromS3(bucketName, localAppId);
        String[] urls = fileContent.split(String.valueOf('\n'));
        for (int i = 0; i < urls.length; i++) {
            createWorkerTask(urls[i], localAppId);
        }
        return urls.length;
    }

    private String readFile(String fileName) {
        String data = "";
        try {
            File myObj = new File(fileName);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                data = data + myReader.nextLine() + "\n";
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return data;
    }

    private void createWorkerTask(String url, String localAppId) {
        //System.out.println("[DEBUG] createWorkerTask");
        aws.sendMessage(aws.getSQSUrl(aws.workerSQSName),
                createImageTaskMsg(url, localAppId));
    }

    private String createImageTaskMsg(String fileUrl, String localAppId) {
        System.out.println("[DEBUG] createImageTaskMsg");
        System.out.println("[DEBUG] fileUrl" + fileUrl);
        String ans = AWS.MessageType.ImageTask +
                aws.delimiter +
                localAppId + aws.delimiter + fileUrl;
        System.out.println("ImageTaskMsg: " + ans);
        return ans;
    }

}