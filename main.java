import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Created by oka on 4/18/17.
 */
public class Ordering {
    //data structures
    int[][] scheduleTable;
    Node OPEN;
    Node[] hashTable;
    int[] processJob;
    int[] processTime;
    int[] parentCount;
    int[] jobTime;
    int[] jobDone;
    int[] jobMarked;

    //IO variables
    Scanner scan1;
    Scanner scan2;
    Scanner scan3;
    PrintWriter out1;
    PrintWriter out2;

    //data variables for the program
    int ProcNeed;
    int ProcUsed;
    int Time;

    public static void main(String args[]) {

        Ordering data = new Ordering(args);
        data.scheduling();
    }

    //Constructor
    public Ordering(String[] args) {
        try {
            //IO variable initialized
            scan1 = new Scanner(new File(args[0]));
            scan2 = new Scanner(new File(args[1]));
            scan3 = new Scanner(System.in);
            out1 = new PrintWriter(new File("out1.txt"));
            out2 = new PrintWriter(new File("out2.txt"));

            int numberNodes;
            numberNodes = scan1.nextInt();

            //hashTable
            hashTable = new Node[numberNodes + 1];
            for (int i = 1; i < hashTable.length; i++) {
                hashTable[i] = new Node(0, 0, null);
            }//creating dummy nodes in each pljaces

            //all other arrays
            processJob = new int[numberNodes + 1];
            processTime = new int[numberNodes + 1];
            jobDone = new int[numberNodes + 1];
            jobMarked = new int[numberNodes + 1];

            parentCount = new int[numberNodes + 1];
            jobTime = new int[numberNodes + 1];

            //Open list
            OPEN = new Node(0, 0, null);//dummy


            //Filling in all other data into respective data structures
            //ProcessJob, processtime, jobDone, JobMarked are not required to fill initially

            //data time file can fill in jobTime and get totaltime for scheduleTable max size
            int totalJobTime = 0;
            int job, time = 0;
            scan2.nextInt(); //skip the first line since it is number of nodes
            while (scan2.hasNext()) {
                job = scan2.nextInt();
                time = scan2.nextInt();
                jobTime[job] = time;

                totalJobTime += time;
            }
            scheduleTable = new int[numberNodes + 1][totalJobTime + 1];


            //data file can fill in parentCount, hashTable
            int parent;
            int child;
            Node track;
            while (scan1.hasNext()) {

                //fill in parent Count
                parent = scan1.nextInt();
                child = scan1.nextInt();
                parentCount[child]++;

                //fill in hashtable
                track = hashTable[parent];
                while (track.next != null) {
                    track = track.next;
                }
                //put it at the end
                Node newNode = new Node(child, jobTime[child], null);
                track.next = newNode;

            }


            //Processors intialzied
            ProcUsed = 0;
            Time = 0;

            System.out.print("Input the number of processor need: ");
            ProcNeed=scan3.nextInt();//reset once u finish the program

            if (ProcNeed > numberNodes) {
                ProcNeed = numberNodes;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Scheduling algorithm
    public void scheduling() {
        while (alljobNotDone()) {
            findOrphans();
            placedOrphansOnProcessors();
            debugPrinting("Printing before incrementing time unit");
            if (cycleExist()) System.exit(0);
            processed();
            processJobPrint();
            debugPrinting("Printing after incrementing time unit");
        }
        printScheduleTable(out1);
        closeIO();
    }

    public void findOrphans() {
        Node track;
        //finding orphan nodes
        for (int currentID = 1; currentID < parentCount.length; currentID++) {
            if (parentCount[currentID] == 0 && jobMarked[currentID] == 0) {//found the orphan node
                jobMarked[currentID] = 1;//marked to ready to process

                Node orphanNode = new Node(currentID, jobTime[currentID], null);

                //put on open list
                track = OPEN;
                while (track.next != null) {
                    track = track.next;
                }//go to the end of open list
                track.next = orphanNode;//put the orphan Node on the end of the open list

            }//no parents and has not processed yet
        }
    }

    public void placedOrphansOnProcessors() {

        int availibleProc = 0;
        Node newJob;


        //while open list is not empty and we use less than what we need
        while (OPEN.next != null && ProcUsed <= ProcNeed) {

            //find an available processor
            availibleProc = findAvailableProcessor();

            if (availibleProc >= 0) {
                newJob = OPEN.next;
                OPEN = OPEN.next;//remove job from open


                processJob[availibleProc] = newJob.jobId;
                processTime[availibleProc] = newJob.time;
                scheduleTable[availibleProc][Time] = processTime[availibleProc];
            } else {

                ProcUsed++;//open up more using processors
                availibleProc = ProcUsed - 1;//id of the new processor

                newJob = OPEN.next;
                OPEN = OPEN.next;//remove job from open

                processJob[availibleProc] = newJob.jobId;
                processTime[availibleProc] = newJob.time;
                scheduleTable[availibleProc][Time] = processTime[availibleProc];
            }
        }
    }

    public int findAvailableProcessor() {
        for (int id = 0; id < ProcUsed - 1; id++) {
            if (processJob[id] <= 0) {
                return id;//unoccupied used processor
            }
        }
        return -1;//all procUsed are occupied
    }



    public boolean cycleExist() {
        if (OPEN.next == null) {//no more orphans
            for (int i = 1; i < jobDone.length; i++) {
                if (jobDone[i] != 1) {//if a job is unfinished aka not Done
                    for (int j = 0; j < ProcUsed; j++) {
                        if (processJob[j] != 0) {
                            return false;
                        }
                    }
                    System.out.println("Error: cycle exist");
                    return true;
                }
            }
        }
        return false;
    }
    public void processed() {
        Time++;
        Node track;
        for (int i = 0; i < processTime.length; i++) {
            if (processJob[i] > 0) {
                processTime[i]--;//decrement all time by 1
                //if finished
                if (processTime[i] == 0) {

                    //decrease the parent count of children from DG
                    track = hashTable[processJob[i]];
                    while (track.next != null) {
                        track = track.next;
                        parentCount[track.jobId]--;
                    }

                    jobDone[processJob[i]] = 1;//update job done
                    processJob[i] = 0;//delete that job from the processor

                }
                scheduleTable[i][Time] = processTime[i];
            }//do this for all finished jobs
        }

    }

    public boolean alljobNotDone() {
        for (int i = 1; i < jobDone.length; i++) {
            if (jobDone[i] != 1) return true;
        }
        return false;
    }

    public void debugPrinting(String message){
        sopl(message);
        printScheduleTable(out2);
        sopl("ProcNeed: "+ ProcNeed);
        sopl("ProcUsed: "+ProcUsed+"\n");
        processJobPrint();
        allprocessTimePrint();
        allParentCountPrint();
        allJobTimePrint();
        jobDonePrint();
        jobMarkedPrint();
    }

    public void closeIO(){
        scan1.close();
        scan2.close();
        scan3.close();
        out1.close();
        out2.close();
    }

    private class Node {
        int jobId;
        int time;
        Node next;

        Node(int id, int time, Node next) {
            this.jobId = id;
            this.time = time;
            this.next = next;
        }

    }

    /*Print Functions for DEBUGGING Purposes*/

    //associate with node start at 1
    public void jobMarkedPrint() {
        sopl("Job Marked Array: ");
        sopl("[Job ID: Mark status]");
        for (int i = 1; i < jobMarked.length; i++) {
            sopl("[ " + i + " : " + jobMarked[i] + "] ");
        }
        sop("\n");
    }

    //associate with processor start at 0
    public void processJobPrint() {
        sopl("Process Job Array: ");
        sopl("[Processor: Job ID]");
        for (int i = 0; i < processJob.length; i++) {
            sopl("[ " + i + " : " + processJob[i] + "] ");
        }
        sop("\n");
    }

    //associate with process start at 0
    public void allprocessTimePrint() {
        sopl("Process time Array");
        sopl("[Job ID: process time]");
        for (int i = 0; i < processTime.length; i++) sopl("[ " + i + " : " + processTime[i] + "] ");
        sop("\n");

    }


    //associate with node start at 1
    public void allJobTimePrint() {
        sopl("Job Time Array:");
        sopl("[Job ID: time]");
        for (int i = 1; i < jobTime.length; i++) {
                sopl("[ " + i + " : " + jobTime[i] + "] ");
            }
        sop("\n");
    }


    //associate with node start at 1
    public void allParentCountPrint() {
        sopl("Parent Count Array:");
        sopl("[Job ID: Parent count]");
        for (int i = 1; i < parentCount.length; i++) sopl("[ " + i + " : " + parentCount[i] + "] ");
        sop("\n");

    }


    //associate with node start at 1
    public void jobDonePrint() {
        sopl(" Jobs Done Array: ");
        sopl("[Job ID: Finish status]");
        for (int i = 1; i < jobDone.length; i++) {
            sopl("[ " + i + " : " + jobDone[i] + "] ");

        }
        sop("\n");
    }

    public void printScheduleTable(PrintWriter out) {
        out.write("Schedule Table:\n");
        out.write("Time " + Time+"\n");
        for (int i = 0; i <= ProcUsed; i++) {
            if (i < 10) out.write("P" + i + "  ");
            else out.write("P" + i + " ");
            for (int j = 0; j <= Time; j++) {
                out.write("[" + scheduleTable[i][j] + "] ");
            }
            out.write("\n");
        }
    }

    public void sop(Object e) {
        out2.write(e+"");
    }

    public void sopl(Object e) {
        out2.write(e+"\n");
    }

}
