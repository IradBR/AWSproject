import software.amazon.awssdk.services.sqs.model.Message;

import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class localApp {
    static AWS aws = AWS.getInstance();
    static String inFilePath;
    static String outFilePath;
    static int tasksPerWorker; //n
    static boolean shouldTerminate;
    static String localAppId;
    static long lastYes;
    static String managerId;

    public static void main(String[] args) {
        //System.out.println("[DEBUG] main of localapp started");
        argsData(args);
        uploadDataToS3();
        localAppId = "localApp" + System.currentTimeMillis();
        //System.out.println("[DEBUG] localAppId: " + localAppId);
        runLocalApp();
    }

    private static void uploadDataToS3() {
        //System.out.println("[DEBUG] main of localapp finished checking argsDATA");
        aws.createBucket(aws.bucketName);
        //System.out.println("[DEBUG] main of localapp finished creating buckets");
        uploadManagerAndWorkerJarIfNotExist();
        //System.out.println("[DEBUG] main of localapp finished uploading manager and worker jar");
    }

    private static void uploadManagerAndWorkerJarIfNotExist() {
        if (!aws.checkIfKeyAlreadyExist(aws.bucketName, aws.managerJarKey) || !aws.checkIfKeyAlreadyExist(aws.bucketName, aws.workerJarKey)) {
            //System.out.println("[DEBUG] uploadManagerAndWorkerJar");
            Path currentDir = Paths.get(".");
            String absolutePath = currentDir.toAbsolutePath().toString();
            String managerJarPath = absolutePath.substring(0, absolutePath.length()-1) + "manager.jar";
            //System.out.println("[DEBUG] managerJarPath " + managerJarPath);
            String workerJarPath = absolutePath + "/worker.jar";
            aws.uploadFileToS3(aws.bucketName, aws.managerJarKey, managerJarPath);
            //System.out.println("[DEBUG] done upload manager jar");
            //System.out.println("[DEBUG] done upload worker jar");
            aws.uploadFileToS3(aws.bucketName, aws.workerJarKey, workerJarPath);
        }
    }


    public static void argsData(String[] args) {
        if (args.length < 3) {
            System.out.println("[ERROR] Missing args. Exiting...");
            System.exit(1);
        } else if (args.length > 4) {
            System.out.println("[ERROR] Too many args. Exiting...");
            System.exit(1);
        } else {
            inFilePath = args[0];
            //System.out.println("[debug] inFilePath: " + inFilePath);
            if (!inFilePath.substring(inFilePath.indexOf('.') + 1).equals("txt")) {
                System.out.println("[ERROR] inFilePath inserted is not a text file. Exiting...");
                System.exit(1);
            }
            outFilePath = args[1];
            if (!outFilePath.substring(outFilePath.indexOf('.') + 1).equals("html")) {
                System.out.println("[ERROR]: outFilePath inserted is not a html file. Exiting...");
                System.exit(1);
            }
            tasksPerWorker = Integer.valueOf(args[2]);
            if (args.length == 4 && args[3].equals("-t")) {
                shouldTerminate = true;
                //System.out.println("[DEBUG] shouldTerminate");
            }
        }
    }

    private static void ifManagerFail(){
        aws.shutdownInstanceByID(managerId);
        createSQS();
        uploadManagerAndWorkerJarIfNotExist();
        createManagerIfNotExists();
        sendNewTask();
        waitForResult();
        if (shouldTerminate) {
            sendTerminateMessage();
        }
        aws.deleteSQS(aws.localAppSQSName + localAppId);
    }

    private static void runLocalApp() {
        createSQS();
        createManagerIfNotExists();
        sendNewTask();
        waitForResult();
        if (shouldTerminate) {
            sendTerminateMessage();
        }
        aws.deleteSQS(aws.localAppSQSName + localAppId);
    }

    private static void createSQS() {
        aws.createSQS(aws.localAppSQSName + localAppId);
        aws.createSQS(aws.managerSQSName);
        aws.createSQS(aws.workerSQSName);
    }

    private static void createManagerIfNotExists() {
        //System.out.println("[DEBUG] Checking if manager exists.");
        if (!aws.checkIfInstanceExist("Manager")) {
            String managerScript = "#!/bin/bash\n" +
                    "echo Manager jar running\n" +
                    "echo s3://"+ aws.bucketName + "/" + aws.managerJarKey + "\n" +
                    "mkdir ManagerFiles\n" +
                    "aws s3 cp s3://"+ aws.bucketName + "/" + aws.managerJarKey + " ./ManagerFiles/" + aws.managerJarName + "\n" +
                    "echo manager copy the jar from s3\n" +
                    "java -jar /ManagerFiles/" + aws.managerJarName + "\n";

            Collection<String> instanceIDs = aws.createInstance(managerScript, "Manager", 1);
            Iterator<String> iter = instanceIDs.iterator();
            if(iter.hasNext()){
                managerId = iter.next();
            }
            else
                managerId = null;
            //System.out.println("[DEBUG] create manager.");
        }
//        else
//            System.out.println("[DEBUG] manager is already exits");
    }

    private static String createNewTaskMessage() {
        //System.out.println("[DEBUG] createNewTaskMessage");
        String message = AWS.MessageType.NewTask +
                aws.delimiter +
                aws.bucketName +
                aws.delimiter +
                localAppId +
                aws.delimiter +
                tasksPerWorker;
        return message;
    }

    private static void sendNewTask() {
        //System.out.println("[DEBUG] sendNewTask");
        aws.uploadFileToS3(aws.bucketName, localAppId, inFilePath);
        aws.sendMessage(aws.getSQSUrl(aws.managerSQSName), createNewTaskMessage());
    }

    private static void saveOutputToFile(String outputFileStr) {
        String htmlTagsOpen = "<!DOCTYPE html><html>  <head>\n" +
                "    <title>The output:</title>\n" +
                "    <style>\n" +
                "      table {\n" +
                "        border-collapse: separate;\n" +
                "        border-spacing: 0px 0;\n" +
                "      }\n" +
                "      th {\n" +
                "        background-color: #4287f5;\n" +
                "        color: white;\n" +
                "      }\n" +
                "      th,\n" +
                "      td {\n" +
                "        width: 150px;\n" +
                "        text-align: center;\n" +
                "        border: 1px solid black;\n" +
                "        padding: 5px;\n" +
                "      }\n" +
                "      h2 {\n" +
                "        color: #4287f5;\n" +
                "      }\n" +
                "    </style>\n" +
                "  </head><body><h1>The output: <p><table><tr><th>Url</th><th>Text image" +
                "</th></tr>";
        String htmlTagsClose = "</table></p></body></html>";
        String[] outputPairs = outputFileStr.split("###");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFilePath));
            writer.write(htmlTagsOpen);
            for (String line : outputPairs) {
                String[] outputLine = line.split("\t");
                String openUrl = "<" + outputLine[0] + "><br>";
                String text = "<p>" + outputLine[1] + "</p>";
                writer.write("<tr><th>" + openUrl + "</th><th>" + text + "</th></tr>");
            }
            writer.write(htmlTagsClose);
            writer.close();
        } catch (Exception e) {
            System.out.println("[ERROR] Exception: " + e.getMessage());
        }
    }


    private static void waitForResult() {
        lastYes = System.currentTimeMillis() + 2000;
        //System.out.println("[DEBUG] waitForResult");
        boolean waitingForResult = true;
        while (waitingForResult) {
            // checking if  the manager is dead
            // the manage send "yes" every 90 seconds
            // if we will not get "yes" after 90 second we will understand that the manager died and we will create new one.
            int milSecInSec = 1000;
            if (System.currentTimeMillis() - lastYes > (milSecInSec * 100)){
                //System.out.println("[DEBUG] last yes was before" +  (System.currentTimeMillis() - lastYes) + "seconds");
                ifManagerFail();
                return;
            }
            List<Message> messages = aws.pullNewMessages(aws.getSQSUrl(aws.localAppSQSName + localAppId));
            if (messages != null) {
                for (Message m : messages) {
                    aws.deleteMessageFromSQS(aws.getSQSUrl(aws.localAppSQSName + localAppId), m);
                    String[] msgParts = m.body().split(aws.delimiter);
                    if (msgParts[0].equals("Yes"))
                        lastYes = System.currentTimeMillis();
                    else {
                        //System.out.println("[DEBUG] got the result: bucketName: " + msgParts[0] + "keyName: " + msgParts[1]);
                        String outputStr = aws.downloadFileFromS3(msgParts[0], msgParts[1]);
                        //System.out.println("[DEBUG] outputStr: " + outputStr);
                        saveOutputToFile(outputStr);
                        waitingForResult = false;
                        break;
                    }
                }
            }
        }
    }

    private static void sendTerminateMessage() {
        aws.sendMessage(aws.getSQSUrl(aws.managerSQSName), AWS.MessageType.Terminate.toString());
        //System.out.println("[DEBUG] sendTerminateMessage");
    }

    private static String readFile(String fileName) {
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
}