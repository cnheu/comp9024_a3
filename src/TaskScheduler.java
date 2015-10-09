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

    /**
     * scheduler - the primary function for TaskScheduler.
     *
     * Time Complexity: O(nlogn)
     *
     * To summarise: The algorithm performs at most 2n priority heap insertions and 2n priority heap removals.
     * This enables the total complexity to stay within O(nlogn) time.
     *
     * populateReleasePQ - performs n priority heap insertions into releasePQ
     * schedulerHelper - performs n priority heap removals from releasePQ, n priority heap insertions into deadlinePQ,
     * n priority heap removals from deadlinePQ and n pushes to the ArrayList schedule.
     *
     * Both populateReleasePQ and schedulerHelper run in O(nlogn) time.
     *
     *
     * @param file1
     * @param file2
     * @param m
     */

    static void scheduler(String file1, String file2, Integer m) {

        HeapPriorityQueue releasePQ = new HeapPriorityQueue(); // O(1)
        HeapPriorityQueue deadlinePQ = new HeapPriorityQueue(); // O(1)
        ArrayList schedule = new ArrayList<>(releasePQ.size()); // O(1) - note size is set to n (number of tasks) to remove need for resizing.

        Path currentRelativePath = Paths.get(""); // O(1)
        String path = currentRelativePath.toAbsolutePath().toString() + "/"; // O(1)
        String scheduleFileName = path + file2 + ".txt"; // O(1)
        File scheduleFile = new File(scheduleFileName); // O(1)

        // Return if file2 already exists.
        if (scheduleFile.exists()) { // O(1)
//            System.out.println("[ERROR] " + scheduleFileName + " (File already exists)");
            return;
        }

        // Return if file1 is invalid.
        if(!populateReleasePQ(releasePQ, file1, path)) return; // O(nlogn) - see function explanation.

        // Return if no feasible schedule exists.
        if (!schedulerHelper(schedule, releasePQ, deadlinePQ, m)) { // O(nlogn) - see function explanation.
            System.out.println("[ERROR] No feasible schedule exists for: " + file1 + " with " + m.toString() + " cores."  );
            return;
        }
        // For Diagnostics
//        for (int i = 0; i < schedule.size(); i ++) {
//            System.out.println(schedule.get(i));
//        }

        populateScheduleFile(schedule, scheduleFile, path);

    }

    /**
     * populateReleasePQ
     *
     * Time Complexity: O(nlogn)
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

            if (inputString == "") throw new Exception("(Input task file empty)");

            String[] inputStringArray = inputString.split("\\W+");  // O(n) - at worst, the split function goes through
                                                                    // each char of the string to match the pattern and
                                                                    // then, appends a substring to the String[] output.
                                                                    // This is proportional to number of tasks n.

            int inputStringLength = inputStringArray.length; // O(1) - used for comparison at the end and checking for complete tasks.
            int counter = 0; // O(1)

            for (String taskAttribute: inputStringArray) {  // O(nlogn) - since there are 3 x n task attributes.
                // Each iteration takes at worst logn time due to the
                // insertion at releasePQ.
                if (taskAttribute.isEmpty()) { // O(1) we want to capture the edge case where the first task is preceded by a whitespace.
                    inputStringLength --;
                    continue;
                }
                if (counter < 3) { // O(1) - this happens every iteration
                    taskStringArray[counter] = taskAttribute;
                    counter ++;
                }
                if (counter == 3) { // O(1) - this only happens once every 3 iterations.
//                    String taskString = Arrays.toString(taskStringArray); // O(1) - used below in the print for diagnostic reasons.
//                    System.out.println(taskString); // print out the taskArray before it's instantiated into a Task object
                    String name = taskStringArray[0]; // O(1)
                    Integer release = Integer.parseInt(taskStringArray[1]); // O(1)
                    Integer deadline = Integer.parseInt(taskStringArray[2]); // O(1)
                    releasePQ.insert(release, new Task(name, release, deadline)); // O(logn) - heap priority queue invariable
                    counter = 0; // O(1)
                }
            }

            if (inputStringLength%3 != 0) throw new Exception("(There is an incomplete task.)");

        }
        catch (FileNotFoundException e) {
            System.out.println("[ERROR] " + e.getMessage());
            return false;
        }
        catch (NumberFormatException e) {
            System.out.println("[ERROR] The task attributes of " + file1 + " do not follow the format required. " + e.getMessage());
            return false;
        }
        catch (Exception e) {
            System.out.println("[ERROR] The task attributes of " + file1 + " do not follow the format required. " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * schedulerHelper
     *
     * Time Complexity: O(nlogn)
     *
     * schedulerHelper achieved O(nlogn) time complexity by ensuring that we run at most n heap insertions and n heap removals.
     *
     * This algorithm consists of the Main Loop (ML), a series of constant time operations (CTOs) and two IF logic statements (IF1 and IF2) .
     *
     * To justify the O(nlogn) complexity consider the below statements:
     *
     * i) IF1 can only run IF !releasePQ.isEmpty. Also each time it runs, it will perform releasePQ.removeMin() and deadlinePQ.insert() exactly ONCE.
     * This means that by definition IF1 will execute n times. The deadlinePQ.insert() call is the most expensive action, so the time complexity of IF1 = O(nlogn)
     *
     * ii) From i), deadlinePQ can have AT MOST n elements. Therefore, IF2 and the FOR loop it contains, because each iteration contains a deadlinePQ.remove() it can only run n times.
     * Since all of the individual operations in IF2 occur in constant time, the total time complexity of IF2 = O(n). (NB: The For loop breaks immediately if deadlinePQ is empty).
     *
     * iii) Since ML can only run when either releasePQ or deadlinePQ is NOT empty, we can say the following. It can run at most n times, whilst !releasePQ.isEmpty().
     * Meanwhile, deadlinePQ can take no more than n extra iterations to empty out. Therefore, we can say ML has an upper bound of 2n iterations.
     *
     * Therefore, taking an upperbound, total number of operations in ML can be said to be <=  (n * IF1) + (n * IF2) + (2n * CTOs)
     *
     * Hence total complexity = O(nlogn) + O(n) + O(2n) = O(nlogn).
     *
     *
     * @param schedule
     * @param releasePQ
     * @param deadlinePQ
     * @param numOfCores
     * @return
     */
    protected static boolean schedulerHelper(ArrayList schedule, HeapPriorityQueue<Integer,Task> releasePQ, HeapPriorityQueue<Integer,Task> deadlinePQ, int numOfCores) {
        int currentTime, releaseTime, peekNextReleaseTime; // O(1)
        currentTime = 0; // O(1)
        releaseTime = 0; // O(1)
        peekNextReleaseTime = 0;

        // MAIN LOOP will run as long as either releasePQ or deadlinePQ is NOT empty.
        while(!releasePQ.isEmpty() || !deadlinePQ.isEmpty()) {

            if (!releasePQ.isEmpty()) releaseTime = releasePQ.min().getKey(); // O(1)

            // IF deadlinePQ is empty, we move the currentTime tracker to the next releaseTime.
            // ELSE, if it isn't empty, that means the tasks of the same releaseTime overflowed the previous scheduling
            // loop. So now we must check that currentTime hasn't gone past the deadline of the earliest first deadline.
            if (deadlinePQ.isEmpty()) {  // O(1)
                currentTime = releaseTime; // O(1)
            }
            else if (currentTime >= deadlinePQ.min().getKey()) { // O(1)
//                System.out.println("Task of scheduling failure was: "+deadlinePQ.min().getValue()); // O(1) - for diagnostics to see where the scheduling failed
                return false;
            }

            // IF Statement #1
            // This IF statement, determines if there are tasks in releasePQ with a releaseTime == currentTime.'
            // We use this so that we can add all tasks with same releaseTime (and any that overflowed the cores in previous loops) to the deadlinePQ.
            if (releaseTime == currentTime && !releasePQ.isEmpty()) {

                HeapPriorityQueue.MyEntry minReleaseEntry = (HeapPriorityQueue.MyEntry) releasePQ.removeMin(); // O(1) - this is a priority queue invariable.
                Task minReleaseTask = (Task) minReleaseEntry.getValue(); // O(1)
                deadlinePQ.insert(minReleaseTask.deadline, minReleaseTask); // O(logn) - heap priority queue invariable
                // IF we've removed the last task from releasePQ and inserted it, we no longer need to keep going.
                if (!releasePQ.isEmpty()) peekNextReleaseTime = releasePQ.min().getKey(); // O(1)
            }

            // IF Statement #2
            // This IF statement, determines whether we have added ALL tasks with the same releaseTime to the deadlinePQ for EDF prioritisation.
            // We know we've added them all IF peekNextReleaseTime revealed that the next task was different.
            if (peekNextReleaseTime != currentTime || releasePQ.isEmpty()) {

                // We proceed to add as many tasks in this currentTime slot as we can by removing them from the deadlinePQ.
                for (int i = 0; i < numOfCores; i ++) {
                    if (deadlinePQ.isEmpty()) break; // O(1)
                    ArrayList taskTime = new ArrayList(); // O(1)
                    taskTime.add(deadlinePQ.removeMin().getValue()); // O(1) - priority queue invariable
                    taskTime.add(currentTime); // O(1)
                    schedule.add(taskTime); // O(1)
                }
                currentTime++;
            }

        }
        return true;
    }

    /**
     * populateScheduleFile
     *
     * Time Complexity: O(n)
     *
     * This function simply iterates through each task on the schedule ArrayList and writes it to the scheduleFile.
     *
     * @param schedule
     * @param scheduleFile
     * @param path
     */
    protected static void populateScheduleFile(ArrayList schedule, File scheduleFile, String path) {
        // Note: we already pre-validate in scheduler the lack of an existing file with name of scheduleFile.
        try{
            scheduleFile.createNewFile(); // O(1)
            FileWriter fw = new FileWriter(scheduleFile); // O(1)
            BufferedWriter bw = new BufferedWriter(fw); // O(1)

            for(int i = 0; i < schedule.size(); i++) { // O(n)
                String taskString = ""; // O(1)
                ArrayList taskTime = (ArrayList) schedule.get(i); // O(1)
                Task task = (Task) taskTime.get(0); // O(1)
                Integer time = (Integer) taskTime.get(1); // O(1)
                taskString += task.toString() + " " + time.toString() + " "; // O(1) - assuming number of characters per task is small compared to n
                bw.write(taskString); // O(1)
            }
            bw.close();
            fw.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{

        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule1", 4);
        /** There is a feasible schedule on 4 cores */

        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule2", 3);
        /** There is no feasible schedule on 3 cores */

        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule3", 5);
        /** There is a feasible scheduler on 5 cores */

        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule4", 4);
        /** There is no feasible schedule on 4 cores */

        /** The sample task sets are sorted. You can shuffle the tasks and test your program again */

        /** MY TESTS */
        TaskScheduler.scheduler("samplefile1_nospaceafterdeadline.txt", "feasibleschedule5", 3);
        TaskScheduler.scheduler("samplefile1_nospaceaftername.txt", "feasibleschedule6", 3);
        TaskScheduler.scheduler("samplefile1_scrambled.txt", "feasibleschedule7", 4);
        TaskScheduler.scheduler("samplefile1_scrambled.txt", "feasibleschedule8", 3);
        TaskScheduler.scheduler("samplefile2_scrambled.txt", "feasibleschedule9", 5);
        TaskScheduler.scheduler("samplefile2_scrambled.txt", "feasibleschedule10", 4);
        TaskScheduler.scheduler("samplefile_empty.txt", "feasibleschedule11", 5);
        TaskScheduler.scheduler("samplefile1_onlyname.txt", "feasibleschedule12", 5);
        TaskScheduler.scheduler("samplefile1_incompletetask.txt", "feasibleschedule13", 4);
        TaskScheduler.scheduler("samplefile3.txt", "feasibleschedule14", 3);
        TaskScheduler.scheduler("samplefile3.txt", "feasibleschedule15", 2);
    }
}

/**
 * Task - a simple class that encapsulates each tasks attributes
 */
class Task {
    String name;
    int release;
    int deadline;

    public Task (String name, int release, int deadline) {
        this.name = name;
        this.release = release;
        this.deadline = deadline;
    }

    // So that each task object returns a string when being printed.
    public String toString() {
        return this.name;
    }
}

/** OLD SECTION DO NOT MARK **/

/**
 * schedulerHelper
 *
 * Time Complexity: O(nlogn)
 *
 * schedulerHelper achieved O(nlogn) time complexity by ensuring that we only process each task ONCE and at most in logn time complexity.
 *
 * At first glance OUTER LOOP and the INNER LOOPs may seem to operate in O(n2) time. However, our controls, specifically of INNER LOOP #1,
 * prevent the total complexity from reaching O(n2).
 *
 * To justify this we need to examine the OUTER LOOP, INNER LOOP #1 and INNER LOOP #2.
 *
 * We observe the OUTER LOOP, runs based on the !releasePQ.isEmpty() || !deadlinePQ.isEmpty() rule.
 * Hence we say that it will run <= 2n times (where n is number of tasks and the max possible size of the priority queues)
 *
 * INNER LOOP #1 [IL1] - takes O(logn) for one loop and runs AT MOST n times.
 * INNER LOOP #2 [IL2] - takes O(1) for one loop and runs AT MOST n times.
 * All Other Operations [Other Operations] - take O(1) time.
 *
 * Total Operations <= (n * [IL1]) + (n * [IL2]) + (2n * [Other Operations])
 * Total Time Complexity <= O(nlogn) + O(n) + O(2n)
 * Simplifies to: O(nlogn)
 *
 * Further examination below shows why IL1 and IL2 both run AT MOST n times.
 *
 * INNER LOOP #1 contains a heap priority queue insertion which is by far the most expensive operation within the OUTER LOOP.
 * Hence it is crucial that it only processes each task once, and is thus initiated a total n times. It contributes a total complexity O(nlogn).
 *
 * We justify this by observing:
 * - One loop of this takes O(logn) time.
 * - Consider two possible scenarios that might cause it to execute:
 *  i) IF !releasePQ.isEmpty()
 *      - Then when currentTime == releaseTime, we releasePQ.removeMin() and deadlinePQ.insert() the task with the next earliest releaseTime.
 *      - We update the releaseTime by peeking at the next earliest releaseTime task with each loop, and we also break out of the loop if releasePQ.isEmpty().
 *      - Each execution results in one task removal from releasePQ.
 *      - Therefore, in this scenario, the number of times it is executed is LIMITED to n (the maximum possible size of releasePQ).
 *  ii) IF !deadlinePQ.isEmpty() AND releasePQ.isEmpty()
 *      - This loop appears to have a chance of being executed IF the parent OUTER LOOP is started by this scenario.
 *      - However, we know releaseTime would NOT have been updated (as per first IF statement on line 189), and hence currentTime will be greater than the previous loop's releaseTime.
 *      - Hence, IL1, only runs in scenario i, a maximum of n times.
 *
 * INNER LOOP #2 ensures EDF rule by removing the task with the next earliest deadline and adding it to the schedule ArrayList.
 * It enables us to add tasks to the schedule in O(n) whilst being independent of the numOfCores.
 * It has a total complexity of O(n).
 *
 * We justify this by observing:
 * - Each loop takes constant time.
 * - It can only happen when !deadlinePQ.isEmpty().
 * - Each execution results in one task removal from deadlinePQ.
 * - Therefore, the number of times it is executed is LIMITED to n (the maximum possible size of deadlinePQ).
 *
 * @param schedule
 * @param releasePQ
 * @param deadlinePQ
 * @param numOfCores
 * @return
 */
//    protected static boolean schedulerHelper(ArrayList schedule, HeapPriorityQueue<Integer,Task> releasePQ, HeapPriorityQueue<Integer,Task> deadlinePQ, int numOfCores) {
//        int currentTime, releaseTime, coresCounter; // O(1)
//        currentTime = 0; // O(1)
//        releaseTime = 0; // O(1)
//
//        // OUTER LOOP #1
//        // We want to continue processing the PQs whilst at least ONE of them is not empty.
//        // Therefore, this OUTER LOOP can run theoretically 2n times.
//        while(!releasePQ.isEmpty() || !deadlinePQ.isEmpty()) {
//            coresCounter = 0; // O(1)
//
//            // IF releasePQ is NOT empty, capture the releaseTime of the next task to be added to deadlinePQ.
//            if (!releasePQ.isEmpty()) releaseTime = releasePQ.min().getKey(); // O(1)
//
//            // IF deadlinePQ is empty, we move the currentTime tracker to the next releaseTime.
//            // ELSE, if it isn't empty, that means the tasks of the same releaseTime overflowed the previous scheduling
//            // loop. So now we must check that currentTime hasn't gone past the deadline of the earliest first deadline.
//            if (deadlinePQ.isEmpty()) {  // O(1)
//                currentTime = releaseTime; // O(1)
//            }
//            else { // O(1)
//                if (currentTime >= deadlinePQ.min().getKey()) { // O(1)
//                    System.out.println("Task of scheduling failure was: "+deadlinePQ.min().getValue());
//                    return false;
//                }
//            }
//
//            // INNER LOOP #1
//            // We want to add all the tasks with releaseTime equal to the currentTime, if the next earliest releaseTime task is > currentTime we don't go through this.
//            while (releaseTime == currentTime && !releasePQ.isEmpty()) {
//
//                HeapPriorityQueue.MyEntry minReleaseEntry = (HeapPriorityQueue.MyEntry) releasePQ.removeMin(); // O(1) - this is a priority queue invariable.
//                Task minReleaseTask = (Task) minReleaseEntry.getValue(); // O(1)
//                deadlinePQ.insert(minReleaseTask.deadline, minReleaseTask); // O(logn) - heap priority queue invariable
//                // IF we've removed the last task from releasePQ and inserted it, we no longer need to keep going.
//                if (releasePQ.isEmpty()) break; // O(1)
//                // We peek at the next earliest releaseTime.
//                releaseTime = releasePQ.min().getKey(); // O(1)
//            }
//
//            // INNER LOOP #2
//            // Removes the task with the next earliest deadline from deadlinePQ and adds it to the schedule in an ArrayList taskTime.
//            while (coresCounter < numOfCores && !deadlinePQ.isEmpty() ) {
////                if (deadlinePQ.isEmpty()) break; // O(1)
//                ArrayList taskTime = new ArrayList(); // O(1)
//                taskTime.add(deadlinePQ.removeMin().getValue()); // O(1) - priority queue invariable
//                taskTime.add(currentTime); // O(1)
//                schedule.add(taskTime); // O(1)
//                coresCounter ++; // O(1)
//            }
//            currentTime ++; // O(1)
//        }
//        return true;
//    }
