/**
 * Created by christophernheu on 27/09/15.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.ArrayList;
//import java.util.
//import java.awt.Entr
import java.util.Map.Entry;


public class TaskScheduler {
    static void scheduler(String file1, String file2, Integer m) {

        TaskHeapPQ releasePQ, deadlinePQ;

        releasePQ = fileToReleasePQ(file1);
//        System.out.println(releasePQ.min());
//        releasePQ.printTasks();
        System.out.println(releasePQ.toString());
//


    }

    /**
     * fileToArray
     *
     * Takes the task input list file and converts it into a PQ for processing
     *
     * @param file1
     * @return
     */
    protected static TaskHeapPQ<Integer,Task> fileToReleasePQ(String file1) {
        file1 = "/Users/christophernheu/IdeaProjects/comp9024_a3/src/" + file1;
        File f = new File(file1);
        TaskHeapPQ<Integer,Task> releasePQ = new TaskHeapPQ<>();
        String[] taskStringArray = new String[3];

        try {
            Scanner input = new Scanner(f);
            String inputString = "";

            while (input.hasNextLine()) {
                inputString += input.nextLine();
            }

            String[] inputStringArray = inputString.split(" ");
            int counter = 0;
//            int testCounter = 0;

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
//                    System.out.println( releasePQ.min().getValue().name);
//                    taskArrayList.add(new Task(name, release, deadline));
                    counter = 0;
                }
            }
        }
        catch (FileNotFoundException e) {
            System.out.println("File wasn't found.");
        }

        return releasePQ;
    }



    public static void main(String[] args) throws Exception{

        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule1", 4);
//        /** There is a feasible schedule on 4 cores */
//        TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule2", 3);
//        /** There is no feasible schedule on 3 cores */
//        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule3", 5);
//        /** There is a feasible scheduler on 5 cores */
//        TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule4", 4);
//        /** There is no feasible schedule on 4 cores */

        /** The sample task sets are sorted. You can shuffle the tasks and test your program again */
    }
}

class Task {
    public String name;
    public int release;
    public int deadline;
//    public String print;

    public Task (String name, int release, int deadline) {
        this.name = name;
        this.release = release;
        this.deadline = deadline;
//        this.print = "[" + this.name + ", " + Integer.toString(this.release) + ", " +
//                Integer.toString(this.deadline) + "]";
    }

    public String toString() {
        return this.name;
    }
}

class TaskHeapPQ<K,V> extends HeapPriorityQueue {
    public TaskHeapPQ() {
        super();
    }
}