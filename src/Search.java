/*
 * 02158 Concurrent Programming, Fall 2021
 * Mandatory Assignment 1
 * Version 1.1
 */


import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.*;

/**
 * Search task. No need to modify.
 */
class SearchTask implements Callable<List<Integer>> {

    char[] text, pattern;
    int from = 0, to = 0; // Searched string: text[from..(to-1)]

    /**
     * Create a task for searching occurrences of 'pattern' in the substring
     * text[from..(to-1)]
     */
    public SearchTask(char[] text, char[] pattern, int from, int to) {
        this.text = text;
        this.pattern = pattern;
        this.from = from;
        this.to = to;
    }

    public List<Integer> call() {
        final int pl = pattern.length;
        List<Integer> result = new LinkedList<Integer>();

        // VERY naive string matching to consume some CPU-cycles
        for (int i = from; i <= to - pl; i++) {
            boolean eq = true;
            for (int j = 0; j < pl; j++) {
                if (text[i + j] != pattern[j])
                    eq = false; // We really should break here
            }
            if (eq)
                result.add(i);
        }

        return result;
    }
}


public class Search {

    static final int max = 10000000; // Max no. of chars searched

    static char[] text = new char[max]; // file to be searched
    static int len;                     // Length of actual text
    static String fname;                // Text file name
    static char[] pattern;              // Search pattern
    static int ntasks = 1;              // No. of tasks
    static int nthreads = 1;            // No. of threads to use
    static boolean printPos = false;    // Print all positions found
    static int warmups = 0;             // No. of warmup searches
    static int runs = 1;                // No. of search repetitions
    static String datafile;            // Name of data file

    static void getArguments(String[] argv) {
        // Reads arguments into static variables
        try {
            int i = 0;

            if (argv.length < 2)
                throw new Exception("Too few arguments");

            while (i < argv.length) {

                /* Check for options */
                if (argv[i].equals("-P")) {
                    printPos = true;
                    i++;
                    continue;
                }

                if (argv[i].equals("-R")) {
                    runs = new Integer(argv[i+1]);
                    i += 2;
                    continue;
                }

                if (argv[i].equals("-W")) {
                    warmups = new Integer(argv[i+1]);
                    i += 2;
                    continue;
                }

                if (argv[i].equals("-d")) {
                    datafile = argv[i+1];
                    i += 2;
                    continue;
                }

                /* Handle positional parameters */
                fname = argv[i];
                pattern = argv[i + 1].toCharArray();
                i += 2;

                if (argv.length > i) {
                    ntasks = new Integer(argv[i]);
                    i++;
                }

                if (argv.length > i) {
                    nthreads = new Integer(argv[i]);
                    i++;
                }

                if (argv.length > i)
                    throw new Exception("Too many arguments");
            }

            /* Read file into memory */
            InputStreamReader file = new InputStreamReader(new FileInputStream(fname));

            len = file.read(text);

            if (file.read() >= 0)
                System.out.println("\nWarning: file truncated to " + max + " characters\n");

            if (ntasks <= 0 || nthreads <= 0 || pattern.length <= 0 || warmups <0 || runs <= 0)
                throw new Exception("Illegal argument(s)");

        } catch (Exception e) {
            System.out.print(e + "\n\nUsage:   java Search <options> file pattern [ntasks [nthreads]] \n\n"
                    + "  where: 0 < nthreads, 0 < ntasks, 0 < size(pattern)\n" + "  Options: \n"
                    + "    -P           Print found positions\n"
                    + "    -W w         Make w warmup searches (w >=0)\n"
                    + "    -R r         Run the search n times (r > 0)\n"
                    + "    -d datafile  Define datafile\n\n" );
            System.exit(1);
        }
    }

    static void writeResult(List<Integer> res) {
        System.out.print("" + res.size() + " occurrences found in ");
        if (printPos) {
            int i = 0;
            System.out.println();
            for (int pos : res) {
                System.out.printf(" %6d", pos);
                if (++i % 10 == 0)
                    System.out.println();
            }
            System.out.println();
        }
    }

    static void writeTime(double time) {
        System.out.printf(String.valueOf(Math.round(time*1000)));
    }

    static void writeRun(int no) {
        System.out.printf("Run no. %2d: ", no);
    }

    static void writeData(String s) {
        try {
            if (datafile != null) {
                // Append result to data file
                FileWriter f = new FileWriter(datafile,true);
                PrintWriter data =  new PrintWriter(new BufferedWriter(f));
                data.println(s);
                data.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void clearData() {
        try {
            if (datafile != null) {
                // Append result to data file
                FileWriter f = new FileWriter(datafile,false);
                PrintWriter data =  new PrintWriter(new BufferedWriter(f));
                data.print("");
                data.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    static long start;
    static double time, totalTime = 0.0;
    static List<Integer> singleResult = null;
    static double singleTime;


    public static void main(String[] argv) {

        /*Problem 1 start*/

        // 1.1 Create runs without warmups
        // -R 30 -d src/Results/P1_1.txt src/DataTestSets/10Mab.txt aaaaaaaaaa
        /*P1(argv); //Uncomment for Problem 1.1*/

        //1.2 Create runs with a fitting amount of warmups
        /* -R 30 -W 10 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa */
        /*P1(argv); //Uncomment for Problem 1.2*/

        /* Problem 1 end */

        /*Problem 2 start*/
        /* -R 30 -W 30 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa 15 */
        /*P2_1(argv);*/

        /* Problem 2 end */

        /* Problem 3 start */

        // 3.1 Small order of growth of tasks
        //-R 30 -W 20 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa 10
        /*P3_1(argv);*/

        // 3.2 Large order of growth of tasks
        //-R 20 -W 20 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa 15
        /*P3_2(argv);*/

        /* Problem 3 end */

        /* Problem 4 start*/

        // 4.1 Small order of growth of tasks and threads

        //-R 20 -W 20 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa 5 5
        /*P4_1(argv);*/

        // 4.2 Large order of growth of tasks and threads

        //-R 20 -W 20 -d src/Results/P1_2.txt src/DataTestSets/10Mab.txt aaaaaaaaaa 4 4

        /*P4_2(argv);*/
        /* Problem 4 End*/

    }

    static void P1(String[] argv){
        if(singleTime==0.0){
            try {
                /* Get and print program parameters */
                getArguments(argv);
                clearData();
                /*System.out.printf("\nProblem%s File=%s, pattern='%s'\nntasks=%d, nthreads=%d, warmups=%d, runs=%d\n",
                        1,fname, new String(pattern), ntasks, nthreads, warmups, runs);*/

                /* Setup execution engine */
                singleThreadExecutor = Executors.newSingleThreadExecutor();

                /**********************************************
                 * Run search using a single task
                 *********************************************/
                SearchTask singleSearch = new SearchTask(text, pattern, 0, len);

                singleResult = null;

                /*
                 * Run a couple of times on engine for loading all classes and
                 * cache warm-up
                 */

                for (int i = 0; i < warmups; i++) {
                    singleThreadExecutor.submit(singleSearch).get();
                }

                /* Run for time measurement(s) and proper result */
                totalTime = 0.0;

                for (int run = 0; run < runs; run++) {
                    start = System.nanoTime();

                    singleResult = singleThreadExecutor.submit(singleSearch).get();

                    time = (double) (System.nanoTime() - start) / 1e9;
                    writeData(String.valueOf(Math.round(time*1000)));
                    totalTime += time;

                    /*System.out.print("\nSingle task: ");*/
                    /*writeRun(run);  writeResult(singleResult);  writeTime(time);*/
                }

                singleTime = totalTime / runs;
                writeData("");

                writeData(String.valueOf(Math.round(time*1000)));

                /*System.out.print("\n\nSingle task (avg.): ");
                writeTime(singleTime);  System.out.println();*/

                /**********************************************
                 * Terminate engine after use
                 *********************************************/
                singleThreadExecutor.shutdown();

            } catch (Exception e) {
                System.out.println("Search: " + e);
            }
        }
    }

    private static void P2_1(String[] argv) {
        P1(argv);

        String[] Argv;
        int NtaskMax = Search.ntasks;
        for(int n = 0; n < NtaskMax+1; n++){
            Argv = new String[]{"-R", Search.runs+"", "-W", Search.warmups+"", "-d", "src/Results/P2_1.txt", Search.fname, new String(Search.pattern), (int) Math.pow(2,n)+""+""};
            P2(Argv);
        }
    }

    static void P2(String[] argv){
        getArguments(argv);
        if(ntasks==1){
            clearData();
        }

        singleThreadExecutor = Executors.newSingleThreadExecutor();
        List<SearchTask> taskList = getSearchTasks();
        List<Integer> result = null;
        try{
            // Run the tasks a couple of times
            for (int i = 0; i < warmups; i++) {
                singleThreadExecutor.invokeAll(taskList);
            }

            totalTime = 0.0;

            for (int run = 0; run < runs; run++) {

                start = System.nanoTime();

                // Submit tasks and await results
                List<Future<List<Integer>>> futures = singleThreadExecutor.invokeAll(taskList);

                // Overall result is an ordered list of unique occurrence positions
                result = new LinkedList<Integer>();
                // Combine future results into an overall result
                for(Future<List<Integer>> f : futures){
                    result.addAll(f.get());
                }

                time = (double) (System.nanoTime() - start) / 1e9;
                totalTime += time;

                /*System.out.printf("\nUsing %2d tasks: ", ntasks);
                writeRun(run);  writeResult(result);  writeTime(time);*/
            }
            double multiTime = totalTime / runs;
            writeData(String.format("%1.2f",singleTime / multiTime));

            if (!singleResult.equals(result)) {
                System.out.println("\nERROR: lists differ");
            }
            System.out.printf("\n\nAverage speedup: %1.2f\n\n", singleTime / multiTime);
        } catch (Exception e) {
            System.out.println("Search: " + e);
        }
        singleThreadExecutor.shutdown();
    }

    private static void P3_1(String[] argv) {
        P1(argv);

        String[] Argv;
        int NtaskMax = Search.ntasks;
        for(int n = 1; n < NtaskMax; n++){
            Argv = new String[]{"-R", Search.runs+"", "-W", Search.warmups+"", "-d", "src/Results/P3_1.txt", Search.fname, new String(Search.pattern), n+""};
            P3(Argv);
        }
    }

    private static void P3_2(String[] argv) {
        P1(argv);

        String[] Argv;
        int NtaskMax = Search.ntasks;
        for(int n = 0; n < NtaskMax+1; n++){
            Argv = new String[]{"-R", Search.runs+"", "-W", Search.warmups+"", "-d", "src/Results/P3_1.txt", Search.fname, new String(Search.pattern), (int) Math.pow(2,n)+""};
            P3(Argv);
        }
    }

    static void P3(String[] argv){
        getArguments(argv);
        if(ntasks==1){
            clearData();
        }

        ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
        List<SearchTask> taskList = getSearchTasks();
        List<Integer> result = null;

        try{
            // Run the tasks a couple of times
            for (int i = 0; i < warmups; i++) {
                newCachedThreadPool.invokeAll(taskList);
            }

            totalTime = 0.0;

            for (int run = 0; run < runs; run++) {

                start = System.nanoTime();

                // Submit tasks and await results
                List<Future<List<Integer>>> futures = newCachedThreadPool.invokeAll(taskList);

                // Overall result is an ordered list of unique occurrence positions
                result = new LinkedList<Integer>();
                // Combine future results into an overall result
                for(Future<List<Integer>> f : futures){
                    result.addAll(f.get());
                }

                time = (double) (System.nanoTime() - start) / 1e9;
                totalTime += time;

                //System.out.printf("\nUsing %2d tasks: ", ntasks);
                /*writeRun(run);  writeResult(result);  writeTime(time);*/
            }

            double multiTime = totalTime / runs;
            writeData(String.format("%1.2f",singleTime / multiTime));
            /*System.out.printf("\n\nUsing %2d tasks (avg.): ", ntasks);
            writeTime(multiTime);  System.out.println();*/


            if (!singleResult.equals(result)) {
                System.out.println("\nERROR: lists differ");
            }
            System.out.printf("\n\nAverage speedup: %1.2f\n\n", singleTime / multiTime);
        }catch (Exception e) {
            System.out.println("Search: " + e);
        }
        newCachedThreadPool.shutdown();

    }

    private static void P4_1(String[] argv) {
        P1(argv);

        int tasksMax = Search.ntasks;
        int ThreadsMax = Search.nthreads;

        String[] Argv;
        double[][] nTasksnThreadsResults = new double[tasksMax][ThreadsMax];
        for(int i = 1; i < tasksMax+1; i++){ // Tasks
            for(int j = 1; j < ThreadsMax+1; j++){ // Threads
                Argv = new String[]{"-R", Search.runs+"", "-W", Search.warmups+"", "-d", "src/Results/P4_1.txt", Search.fname, new String(Search.pattern), i+"", j+""};
                nTasksnThreadsResults[i-1][j-1] = P4(Argv);
            }
        }
        clearData();
        for (double[] list:nTasksnThreadsResults) {
            System.out.println(Arrays.toString(list));
            writeData(Arrays.toString(list));
        }
    }

    private static void P4_2(String[] argv) {
        P1(argv);

        int tasksMax = Search.ntasks;
        int ThreadsMax = Search.nthreads;
        String[] Argv;
        double[][] nTasksnThreadsResults = new double[tasksMax+1][ThreadsMax+1];

        for(int i = 0; i < tasksMax+1; i++){ // Tasks
            for(int j = 0; j < ThreadsMax+1; j++){ // Threads
                Argv = new String[]{"-R", Search.runs+"", "-W", Search.warmups+"", "-d", "src/Results/P4_2.txt", Search.fname, new String(Search.pattern), (int) Math.pow(4,i)+"", (int) Math.pow(4,j)+""};
                nTasksnThreadsResults[i][j] = P4(Argv);
                System.out.println("i: " + i +" j: " + j + " 4^i: " + Math.pow(4,i) + " 4^j: " + Math.pow(4,j));

            }
        }
        clearData();
        for (double[] list:nTasksnThreadsResults) {
            System.out.println(Arrays.toString(list));
            writeData(Arrays.toString(list));
        }
    }

    static double P4(String[] argv){
        getArguments(argv);
        double multiTime = 0;

        if(ntasks==1 && nthreads==1){
            clearData();
        }

        ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(nthreads);
        List<SearchTask> taskList = getSearchTasks();
        List<Integer> result = null;

        try{
            // Run the tasks a couple of times
            for (int i = 0; i < warmups; i++) {
                newFixedThreadPool.invokeAll(taskList);
            }

            totalTime = 0.0;

            for (int run = 0; run < runs; run++) {

                start = System.nanoTime();

                // Submit tasks and await results
                List<Future<List<Integer>>> futures = newFixedThreadPool.invokeAll(taskList);

                // Overall result is an ordered list of unique occurrence positions
                result = new LinkedList<Integer>();
                // Combine future results into an overall result
                for(Future<List<Integer>> f : futures){
                    result.addAll(f.get());
                }

                time = (double) (System.nanoTime() - start) / 1e9;
                totalTime += time;

                //System.out.printf("\nUsing %2d tasks: ", ntasks);
                /*writeRun(run);  writeResult(result);  writeTime(time);*/
            }

            multiTime = totalTime / runs;
            //writeTime(multiTime);  System.out.println();


            if (!singleResult.equals(result)) {
                System.out.println("\nERROR: lists differ");
            }

            System.out.printf("\n\nAverage speedup: %1.2f\n\n", singleTime / multiTime);
        }catch (Exception e) {
            System.out.println("Search: " + e);
        }
        newFixedThreadPool.shutdown();

        return Double.parseDouble(String.format("%1.2f",singleTime / multiTime));
    }

    private static List<SearchTask> getSearchTasks() {
        // Create list of tasks
        List<SearchTask> taskList = new ArrayList<SearchTask>();
        // Add tasks to list here
        double split = (double) len / (double) ntasks;

        for(int i = 0; i<ntasks-1; i++){
            taskList.add(
                    new SearchTask(
                            text,
                            pattern,
                            (int) (i*split),
                            (int) (((i+1)*split)+(pattern.length-1))
                    )
            );
        }
        taskList.add(
                new SearchTask(
                        text,
                        pattern,
                        (int) (split*(ntasks-1)),
                        len
                )
        );
        return taskList;
    }
}