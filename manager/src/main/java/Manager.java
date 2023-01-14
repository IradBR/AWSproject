import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Manager {
    public static final int MAX_INSTANCES = 15; //19-1 (manager)
    public static final int MAX_NUM_OF_THREADS = 10;
    private static AWS aws;


    public static void main(String[] args) {
        System.out.println("[DEBUG] created a manager");
        AWS aws = AWS.getInstance();
        Map<String, LocalAppInfo> localAppsInfo = new ConcurrentHashMap<>();
        Queue<LocalAppInfo> numOfWorkersQueue =
                new PriorityQueue<>((localApp1, localApp2) -> localApp2.getNumOfworkers() - localApp1.getNumOfworkers());
        AtomicBoolean shouldTerminate = new AtomicBoolean(false);
        AtomicBoolean isTerminated = new AtomicBoolean(false);
        Object lock = new Object();

        ExecutorService executorService = Executors.newFixedThreadPool(MAX_NUM_OF_THREADS);
        System.out.println("[DEBUG] create manager threads");
        for (int i = 0; i < MAX_NUM_OF_THREADS; i++) {
            executorService.execute(new ManagerThread(aws, localAppsInfo, shouldTerminate, numOfWorkersQueue, isTerminated, lock));
        }

        Thread managerIsAliveThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!shouldTerminate.get()) {
                        Iterator iter = localAppsInfo.keySet().iterator();
                        while (iter.hasNext())
                            aws.sendMessage(aws.getSQSUrl(aws.localAppSQSName + iter.next()), "Yes");
                    }
                    else
                        timer.cancel();
                }

            }, 0, 10 * 1000); // every 10 second the run mission is executed - ping sent
        });
        managerIsAliveThread.start();

        try {
            executorService.awaitTermination(1, TimeUnit.SECONDS);
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}