/**
 * Created by christophernheu on 27/09/15.
 */
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.io.FileNotFoundException;
import java.util.*;


public class TaskScheduler {
    static void scheduler(String file1, String file2, Integer m) {

        HeapPriorityQueue releasePQ = new HeapPriorityQueue(); // O(1)
        HeapPriorityQueue deadlinePQ = new HeapPriorityQueue(); // O(1)
        ArrayList schedule = new ArrayList<>(releasePQ.size()); // O(1)

        Path currentRelativePath = Paths.get(""); // O(1)
        String path = currentRelativePath.toAbsolutePath().toString() + "/"; // O(1)
        String scheduleFileName = path + file2 + ".txt"; // O(1)
        File scheduleFile = new File(scheduleFileName); // O(1)

        // Return if file2 already exists.
        if (scheduleFile.exists()) { // O(1)
            System.out.println("[ERROR] " + scheduleFileName + " (File already exists)");
            return;
        }

        // Return if file1 is invalid.
        if(!populateReleasePQ(releasePQ, file1, path)) return; // O(logn)

        // Return if no feasible schedule exists.
        if (!schedulerHelper(schedule, releasePQ, deadlinePQ, m)) {
            System.out.println("[ERROR] No feasible schedule exists for: " + file1 + " with " + m.toString() + " cores."  );
            return;
        }

        for (int i = 0; i < schedule.size(); i ++) {
            System.out.println(schedule.get(i));
        }

        populateScheduleFile(schedule, scheduleFile, path);

    }

    /**
     * populateReleasePQ
     *
     * Complexity: O(nlogn)
     *
     * Takes the input task list from file1 and passes the tasks in their native order into the releasePQ.
     * We must take the assumption that the number of characters used to describe each task is small compared to n.
     *
     * @param releasePQ
     * @param file1
     * @param path
     * @return
     */
    protected static boolean populateReleasePQ(HeapPriorityQueue releasePQ, String file1, String path) {
        file1 = path + file1;
        File f = new File(file1);
        String[] taskStringArray = new String[3];

        try {
            Scanner input = new Scanner(f); // O(1)
            String inputString = ""; // O(1)

            while (input.hasNextLine()) { // O(n) - at worst, if each task attribute is on a separate line
                inputString += input.nextLine() + "\n"; // O(1)
            }

            String[] inputStringArray = inputString.split("\\W+");  // O(n) - at worst, the split function goes through
                                                                    // each char of the string to match the pattern and
                                                                    // then, appends a substring to the String[] output.
                                                                    // This is proportional to number of tasks n.
            int counter = 0; // O(1)

            for (String taskAttribute: inputStringArray) {  // O(nlogn) - since there are 3 x n task attributes.
                                                            // Each iteration takes at worst logn time due to the
                                                            // insertion at releasePQ.

                if (counter < 3) { // O(1) - this happens every iteration
                    taskStringArray[counter] = taskAttribute;
                    counter ++;
                }
                if (counter == 3) { // O(1) - this only happens once every 3 iterations.
                    String taskString = Arrays.toString(taskStringArray); // O(1)
                    System.out.println(taskString); // print out the taskArray before it's instantiated into a Task object
                    String name = taskStringArray[0]; // O(1)
                    Integer release = Integer.parseInt(taskStringArray[1]); // O(1)
                    Integer deadline = Integer.parseInt(taskStringArray[2]); // O(1)

                    releasePQ.insert(release, new Task(name, release, deadline)); // O(logn)
                    counter = 0; // O(1)
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
     * schedulerHelper
     *
     * Time Complexity: O(nlogn)
     *
     * schedulerHelper achieved O(nlogn) time complexity by ensuring that we only process each task ONCE and at most in logn time complexity.
     *
     * At first glance OUTER LOOP and the INNER LOOPs may seem to operate in O(n2) time. However,
     * Our controls, specifically of INNER LOOP #1, prevent the INNER LOOPs from running more than n times.
     *
     * To justify this we need to examine INNER LOOP #1 and INNER LOOP #2.
     *
     * INNER LOOP #1 ensures that we only process each task once, and is initiated a total n times. It has a total complexity O(nlogn).
     * We justify this by observing:
     * - One loop of this takes O(logn) time.
     * - Consider two possible scenarios for intiation:
     * i) IF !releasePQ.isEmpty()
     * - Then when currentTime == releaseTime, we removeMin and insert into deadlinePQ the task with the next earliest releaseTime.
     * - We update the releaseTime by peeking at the next earliest releaseTime task with each loop, and we also stop running if releasePQ is empty.
     * - Therefore, based on this scenario, it can run at most n times, where n is the size of releasePQ.
     * ii) IF !deadlinePQ.isEmpty()
     * - This loop has a chance of being initiated IF the parent OUTER LOOP is started by this scenario.
     * - However in this case, we know releaseTime would NOT have been updated (as per first IF statement), and currentTime will be greater than the previous releaseTime.
     * - Hence, INNER LOOP #1, only runs in scenario i, a maximum of n times.
     *
     * INNER LOOP #2 ensures EDF by removing next earliest ready task and adding it to the schedule array, it also happens at most n times.
     * It enables us to add tasks to the schedule in n-time whilst being independent of the numOfCores.
     * It uses the numOfCores, to tell us when we need to advance to the next time step,
     * It has a total complexity of O(n).
     * We justify this by observing:
     * - Each loop takes constant time.
     * - It does not happen if deadlinePQ is empty.
     *
     * @param schedule
     * @param releasePQ
     * @param deadlinePQ
     * @param numOfCores
     * @return
     */
    protected static boolean schedulerHelper(ArrayList schedule, HeapPriorityQueue<Integer,Task> releasePQ, HeapPriorityQueue<Integer,Task> deadlinePQ, int numOfCores) {
        int currentTime, releaseTime, coresCounter; // O(1)
        currentTime = 0; // O(1)
        releaseTime = 0; // O(1)

        // OUTER LOOP #1
        // We want to continue processing the PQs whilst at least ONE of them is not empty.
        while(!releasePQ.isEmpty() || !deadlinePQ.isEmpty()) {
            coresCounter = 0; // O(1)

            // IF releasePQ is NOT empty, capture the releaseTime of the next task to be added to deadlinePQ.
            if (!releasePQ.isEmpty()) releaseTime = (Integer) releasePQ.min().getKey(); // O(1)

            // IF deadlinePQ is empty, we move the currentTime tracker to the next releaseTime.
            // ELSE, if it isn't empty, that means the tasks of the same releaseTime overflowed the previous scheduling
            // loop. So now we must check that currentTime hasn't gone past the deadline of the earliest first deadline.
            if (deadlinePQ.isEmpty()) {  // O(1)
                currentTime = releaseTime; // O(1)
            }
            else { // O(1)
                if (currentTime >= (Integer) deadlinePQ.min().getKey()) { // O(1)
                    System.out.println("Point of scheduling failure was: "+deadlinePQ.min().getValue());
                    return false;
                }
            }

            // INNER LOOP #1
            // We want to add all the tasks with releaseTime equal to the currentTime, if the next earliest releaseTime task is > currentTime we don't go through this..
            while (releaseTime == currentTime) {

                HeapPriorityQueue.MyEntry minReleaseEntry = (HeapPriorityQueue.MyEntry) releasePQ.removeMin(); // O(1) - this is a priority queue invariable.
                Task minReleaseTask = (Task) minReleaseEntry.getValue(); // O(1)
                deadlinePQ.insert(minReleaseTask.deadline, minReleaseTask); // O(logn)
                // IF we've removed the last task from releasePQ and inserted it, we no longer need to keep going.
                if (releasePQ.isEmpty()) break; // O(1)
                // We peek at the next earliest releaseTime.
                releaseTime = (Integer) releasePQ.min().getKey(); // O(1)
            }

            // INNER LOOP #2
            // Removes the task with the next earliest deadline from deadlinePQ and adds it to the schedule in an ArrayList taskTime.
            // Removes from deadlinePQ and adds it to schedule. If deadlinePQ still has members we will assess it with the next round of tasks of same releaseTime.
            while (coresCounter < numOfCores && !deadlinePQ.isEmpty() ) {
//                if (deadlinePQ.isEmpty()) break; // O(1)
                ArrayList taskTime = new ArrayList(); // O(1)
                taskTime.add(deadlinePQ.removeMin().getValue()); // O(1) - priority queue invariable
                taskTime.add(currentTime); // O(1)
                schedule.add(taskTime); // O(1)
                coresCounter ++; // O(1)
            }
            currentTime ++; // O(1)
        }
        return true;
    }

    /**
     *
     * @param schedule
     * @param scheduleFile
     * @param path
     */
    protected static void populateScheduleFile(ArrayList schedule, File scheduleFile, String path) {

        try{
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

        /** The sample task sets are sorted. You can shuffle the tasks and test your program again */

        /** MY TESTS */
//        TaskScheduler.scheduler("samplefile1_nospaceafterdeadline.txt", "feasibleschedule5", 3);
//        TaskScheduler.scheduler("samplefile1_nospaceaftername.txt", "feasibleschedule6", 3);
        TaskScheduler.scheduler("samplefile2_scrambled.txt", "feasibleschedule6", 5);

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