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

        HeapPriorityQueue releasePQ, deadlinePQ;

        releasePQ = fileToReleasePQ(file1);
//        ArrayList tasks = fileToArray(file1);

    }

    /**
     * fileToArray
     *
     * Takes the task input list file and converts it into a PQ for processing
     *
     * @param file1
     * @return
     */
    protected static HeapPriorityQueue<Integer,Task> fileToReleasePQ(String file1) {
        file1 = "/Users/christophernheu/IdeaProjects/comp9024_a3/src/" + file1;
        File f = new File(file1);
//        ArrayList<Task> taskArrayList = new ArrayList<>();
        HeapPriorityQueue<Integer,Task> releasePQ = new HeapPriorityQueue();
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

    public Task (String name, int release, int deadline) {
        this.name = name;
        this.release = release;
        this.deadline = deadline;
    }
}

class TaskQueue<K,V> extends HeapPriorityQueue {

    public TaskQueue() {
        super();
    }
}






//    protected static class PQEntry<K,V> implements Entry<K,V> {
//        private K k; // key
//        private V v; // value
//
//        public PQEntry(K key, V value) {
//            k = key;
//            v = value;
//        }
//
//        // methods of the Entry interface
//        public K getKey() {
//            return k;
//        }
//
//        public V getValue() {
//            return v;
//        } // utilities not exposed as part of the Entry interface
//
//        protected void setKey(K key) {
//            k = key;
//        }
//
//        protected void setValue(V value) {
//            v = value;
//        }
//    }
//
//    protected ArrayList<Entry<K,V>> heap = new ArrayList<>();
//
//
//    protected int parent(int j) { return (j−1) / 2; } // truncating division
//    protected int left(int j) { return 2*j + 1; }
//    protected int right(int j) { return 2*j + 2; }
//    protected boolean hasLeft(int j) { return left(j) < heap.size(); }
//    protected boolean hasRight(int j) { return right(j) < heap.size(); }
//
//    protected void swap(int i, int j) {
//        Entry<K,V> temp = heap.get(i); heap.set(i, heap.get(j)); heap.set(j, temp);
//    }
//
//    protected void upheap(int j) {
//        while (j > 0) { // continue until reaching root (or break statement) int p = parent(j);
//            if (compare(heap.get(j), heap.get(p)) >= 0) break; // heap property verified swap(j, p);
//            j = p; // continue from the parent's location }
//        }
//    }
//
//    protected void downheap(int j) {
//        while (hasLeft(j)) {
//            // continue to bottom (or break statement) // although right may be smaller
//
//            int leftIndex = left(j);
//            int smallChildIndex = leftIndex;
//            if (hasRight(j)) {
//                int rightIndex = right(j);
//                if (compare(heap.get(leftIndex), heap.get(rightIndex)) > 0)
//                    smallChildIndex = rightIndex; // right child is smaller
//            }
//            if (compare(heap.get(smallChildIndex), heap.get(j)) >= 0)
//                break;
//            swap(j, smallChildIndex);
//            j = smallChildIndex;
//        }
//    }
//
//    // public methods
//    public int size() { return heap.size();}
//
//    public Entry<K,V> min() {
//        if (heap.isEmpty()) return null;
//        return heap.get(0);
//    }
//
//    //TODO: Add insert functionality
//    public Entry<K,V> insert(K key, V value) throws IllegalArgumentException {
////        checkKey(key);
//        Entry<K,V> newest = new PQEntry<>(key, value);
//        heap.add(newest);
//        upheap(heap.size() - 1);
//        return newest;
//    }
//
//    //TODO: Add remove functionality
//    public Entry<K,V> removeMin() {
//        if (heap.isEmpty()) return null;
//        Entry<K,V> answer = heap.get(0);
//        swap(0, heap.size() - 1);
//        downheap(0);
//        return answer;
//    }
//
//    //TODO: Add bubbleUp functionality
//    protected void()
//    //TODO: Add bubbleDown functionality