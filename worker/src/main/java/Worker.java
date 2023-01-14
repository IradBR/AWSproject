import com.asprise.ocr.Ocr;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker {
    static AtomicInteger counter = new AtomicInteger(0);

    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) throws IOException {
        start();
    }

    private static void start() throws IOException {
        System.out.println("[DEBUG] start a worker");
        while(true) {
            Message message = aws.getNewMessage(aws.getSQSUrl(aws.workerSQSName));
//            System.out.println("the
//            worker is handling message: " + message.body());
            if (message != null) {
                AtomicBoolean isFinished = new AtomicBoolean(false);
                makeMessageVisibilityDynamic(message, aws.getSQSUrl(aws.workerSQSName), isFinished);
                String[] properties = message.body().split("#");
                System.out.println(Arrays.toString(properties));
                if (properties[0].equals("ImageTask")) {
                    System.out.println("[DEBUG] The message is Image Task");
                    String localAppId = properties[1];
                    String url = properties[2];
                    int imageCounter = counter.incrementAndGet();
                    String imageAddress = imageCounter + ".png";
                    // next 6 line for downloading and OCRing the image:
                    String returnValue = downloadImage(url, imageAddress);
                    String text;
                    if (!returnValue.equals("Success"))
                        text = returnValue;
                    else
                        text = OCRImage(imageAddress);
                    if(text.equals(""))
                        text = "OCR library ERROR, can't process image";
                    //Worker puts a message in an SQS queue indicating the original URL of the image and the extracted text:
                    aws.sendMessage(aws.getSQSUrl(aws.managerSQSName), createDoneOCRMessage(localAppId, url, text));
                    isFinished.set(true);
                    aws.deleteMessageFromSQS(aws.getSQSUrl(aws.workerSQSName), message);
                } else { //else, its a terminate msg
                    System.out.println("[DEBUG] terminate");
                    aws.deleteMessageFromSQS(aws.getSQSUrl(aws.workerSQSName), message);
                    aws.shutdownInstance();
                    break;
                }
            }
        }
    }

    //message visibility time dynamic
    private static void makeMessageVisibilityDynamic(Message message, String workerSQSUrl, AtomicBoolean isFinished) {
        String receipt = message.receiptHandle();
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!isFinished.get()) // not finished the work need to increase the visibility time
                        aws.changeMessageVisibility(workerSQSUrl, receipt);
                    else {
                        timer.cancel();
                    }
                }
            }, 0, 90 * 1000); // every 90 second the run mission is executed - increase the isibility time
        });
        timerThread.start();
    }

    private static String downloadImage(String imageUrl, String destinationFile) throws IOException {
//        System.out.println("[DEBUG] Function: downloadImage. Args: " + imageUrl + ", " + destinationFile);
        try {
            URL url = new URL(imageUrl);
            InputStream is = url.openStream();
            OutputStream os = new FileOutputStream(destinationFile);

            byte[] bytes = new byte[2048];
            int length;

            while ((length = is.read(bytes)) != -1) {
                os.write(bytes, 0, length);
            }

            is.close();
            os.close();
        }
        catch(Exception e){
            return "Problem downloading the image from given URL. Description: " + e.toString();
        }
        return "Success";
    }

    private static String OCRImage(String imageUrl) throws IOException {
        System.out.println("[DEBUG] Function: OCR image. Args: " + imageUrl);
        Ocr.setUp(); // one time setup
        Ocr ocr = new Ocr(); // create a new OCR engine
        String result;
        try {
            ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
            result = ocr.recognize(new File[]{new File(imageUrl)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
            ocr.stopEngine();
        } catch (Exception s){
            result = s.toString();
        } catch (Error e){
            result = e.toString();
        }
        return result;
    }

    private static String createDoneOCRMessage(String localAppId, String url, String text) {
        System.out.println("[DEBUG] createDoneOCRMessage");
        String ans = AWS.MessageType.DoneOCR +
                aws.delimiter +
                localAppId +
                aws.delimiter +
                url +
                aws.delimiter +
                text;
        System.out.println("createDoneOCRMessage is sending to manager: " + ans);
        return ans;
    }

}
