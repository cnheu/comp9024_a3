/**
 * Created by christophernheu on 27/09/15.
 */
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.BufferedWriter;
//import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.FileNotFoundException;
import java.util.*;
//import java.util.
//import java.awt.Entr


public class TaskScheduler {
    static void scheduler(String file1, String file2, Integer m) {
        Path currentRelativePath = Paths.get("");
        String path = currentRelativePath.toAbsolutePath().toString() + "/";

        HeapPriorityQueue releasePQ = new HeapPriorityQueue();
        HeapPriorityQueue deadlinePQ = new HeapPriorityQueue();

        if(!populateReleasePQ(releasePQ, file1, path)) return;

        ArrayList schedule = new ArrayList<>(releasePQ.size());

        if (!schedulerHelper(schedule, releasePQ, deadlinePQ, m)) {
            System.out.println("[ERROR] No feasible schedule exists for: " + file1 + " with " + m.toString() + " cores."  );
            return;
        }

        for (int i = 0; i < schedule.size(); i ++) {
            System.out.println(schedule.get(i));
        }

        createScheduleFile(schedule, file2, path);

    }

    protected static boolean schedulerHelper(ArrayList schedule, HeapPriorityQueue releasePQ, HeapPriorityQueue deadlinePQ, int numOfCores) {
        int currentTime, releaseTime, coresCounter;
        currentTime = 0;
        releaseTime = 0;

        // If either of them are NOT empty, keep going through.
        while(!releasePQ.isEmpty() || !deadlinePQ.isEmpty()) {
            coresCounter = 0;
            if (!releasePQ.isEmpty()) releaseTime = (Integer) releasePQ.min().getKey();

            // Accounts for if the cores were overflowed during previous currentTime value
            if (deadlinePQ.isEmpty()) {
                currentTime = releaseTime;
            }
            else {
                if (currentTime >= (Integer) deadlinePQ.min().getKey()) {
//                    System.out.println("Point of scheduling failure was: "+deadlinePQ.min().getValue());
                    return false;
                }
            }

            // Adds on to deadlinePQ and assesses all the tasks of equal releaseTime
            while (releaseTime == currentTime) {

                HeapPriorityQueue.MyEntry minReleaseEntry = (HeapPriorityQueue.MyEntry) releasePQ.removeMin();
                Task minReleaseTask = (Task) minReleaseEntry.getValue();
                deadlinePQ.insert(minReleaseTask.deadline, minReleaseTask);
                if (releasePQ.isEmpty()) break;
                releaseTime = (Integer) releasePQ.min().getKey();
            }

            // Removes from deadlinePQ and adds it to schedule. If deadlinePQ still has members we will assess it with the next round of tasks of same releaseTime.
            while (coresCounter < numOfCores) {
                if (deadlinePQ.isEmpty()) break;
                ArrayList taskTime = new ArrayList();
                taskTime.add((Task) deadlinePQ.removeMin().getValue());
                taskTime.add(currentTime);
                schedule.add(taskTime);
                coresCounter ++;
            }
            currentTime ++;
        }
        return true;
    }

    /**
     * populateReleasePQ
     *
     * Takes the task input list file and converts it into a PQ for processing
     *
     * @param file1
     * @return
     */
    protected static boolean populateReleasePQ(HeapPriorityQueue releasePQ, String file1, String path) {
        file1 = path + file1;
        File f = new File(file1);
        String[] taskStringArray = new String[3];

        try {
            Scanner input = new Scanner(f);
            String inputString = "";

            while (input.hasNextLine()) {
                inputString += input.nextLine() + "\n";
            }

            String[] inputStringArray = inputString.split("\\W+");
            int counter = 0;

            for (String taskDatum: inputStringArray) {
                if (counter < 3) {
                    taskStringArray[counter] = taskDatum;
                    counter ++;
                }
                if (counter == 3) {
                    String taskString = Arrays.toString(taskStringArray);
                    System.out.println(taskString); // print out the taskArray before it's instantiated into a Task object
                    String name = taskStringArray[0];
                    Integer release = Integer.parseInt(taskStringArray[1]);
                    Integer deadline = Integer.parseInt(taskStringArray[2]);

                    releasePQ.insert(release, new Task(name, release, deadline));
                    counter = 0;
                }
            }
        }
        catch (FileNotFoundException e) {
            System.out.println("[ERROR] " + e.getMessage());
            return false;
        }
        catch (NumberFormatException e) {
            System.out.println("[ERROR] the task attributes of " + file1 + " do not follow the format required. " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     *
     * @param schedule
     * @param file2
     * @param path
     */
    protected static void createScheduleFile(ArrayList schedule, String file2, String path) {
        String scheduleFileName = path + file2 + ".txt";

        try{
            File scheduleFile = new File(scheduleFileName);
            if (scheduleFile.exists()) throw new Exception("File already exists.");
            scheduleFile.createNewFile();
            FileWriter fw = new FileWriter(scheduleFile);
            BufferedWriter bw = new BufferedWriter(fw);

            for(int i = 0; i < schedule.size(); i++) {
                String taskString = "";
                ArrayList taskTime = (ArrayList) schedule.get(i);
                Task task = (Task) taskTime.get(0);
                Integer time = (Integer) taskTime.get(1);
                taskString += task.toString() + " " + time.toString() + " ";
                bw.write(taskString);
            }
            bw.close();
            fw.close();

        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(Exception e) {
            System.out.println("[ERROR] " + scheduleFileName + " (File already exists)");
        }
    }

    public static void main(String[] args) throws Exception{

//        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule1", 4);
        /** There is a feasible schedule on 4 cores */

//        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule2", 3);
        /** There is no feasible schedule on 3 cores */

//        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule3", 5);
        /** There is a feasible scheduler on 5 cores */

//        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule4", 4);
        /** There is no feasible schedule on 4 cores */
//        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule5", 3);

        /** The sample task sets are sorted. You can shuffle the tasks and test your program again */

        /** MY TESTS */
//        TaskScheduler.scheduler("samplefile1_nospaceafterdeadline.txt", "feasibleschedule2", 3);
//        TaskScheduler.scheduler("samplefile1_nospaceaftername.txt", "feasibleschedule2", 3);

    }
}

class Task {
    public String name;
    public int release;
    public int deadline;

    public Task (String name, int release, int deadline) {
        this.name = name;
        this.release = release;
        this.deadline = deadline;
    }

    public String toString() {
        return this.name;
    }
}