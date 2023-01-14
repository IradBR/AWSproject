import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalAppInfo {

    private Map<String, String> tasksResult;
    private AtomicInteger numOfTaskLeft;
    private final int numOfworkers;

    public LocalAppInfo(int numOfTaskLeft, int numOfworkers) {
        this.tasksResult = new ConcurrentHashMap<>();
        this.numOfTaskLeft = new AtomicInteger(numOfTaskLeft);
        this.numOfworkers = numOfworkers;
    }

    public Map<String, String> getTasksResult() {
        return tasksResult;
    }

    public void setTasksResult(Map<String, String> tasksResult) {
        this.tasksResult = tasksResult;
    }

    public AtomicInteger getNumOfTaskLeft() {
        return numOfTaskLeft;
    }

    public void setNumOfTaskLeft(AtomicInteger numOfTaskLeft) {
        this.numOfTaskLeft = numOfTaskLeft;
    }

    public int getNumOfworkers() {
        return numOfworkers;
    }

}
