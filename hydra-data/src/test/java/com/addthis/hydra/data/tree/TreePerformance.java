/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.data.tree;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.addthis.basis.util.Files;

import com.google.common.collect.Iterators;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

public class TreePerformance {

    private static enum Operation {
        INSERT, GET, DELETE
    }

    private static enum Type {
        TREE, CONCURRENT_TREE
    }

    private static class NullOutputStream extends OutputStream {

        public void write(int i) throws IOException {
            //do nothing
        }
    }

    private static int numPagesInMemory = 500;

    private static File makeTemporaryDirectory() throws IOException {
        final File temp;

        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return temp;
    }

    static final class TestThread implements Runnable {

        final CountDownLatch startBarrier;
        final CountDownLatch stopBarrier;
        final int myId;
        final List<String> values;
        final List<Operation> operations;
        final int[] threadId;
        final DataTree cache;
        int counter;

        public TestThread(
                final CountDownLatch startBarrier,
                final CountDownLatch stopBarrier,
                final int id,
                List<String> values,
                List<Operation> operations, int[] threadId,
                DataTree cache) {
            this.startBarrier = startBarrier;
            this.stopBarrier = stopBarrier;
            this.myId = id;
            this.values = values;
            this.operations = operations;
            this.threadId = threadId;
            this.cache = cache;
            this.counter = 0;
        }

        @Override
        public void run() {
            try {
                startBarrier.countDown();
                startBarrier.await();
                for (int i = 0; i < threadId.length; i++) {
                    if (threadId[i] == myId) {
                        String valString = values.get(i);
                        Operation op = operations.get(i);
                        switch (op) {
                            case GET:
                                cache.getNode(valString);
                                break;
                            case INSERT:
                                DataTreeNode node = cache.getOrCreateNode(valString, null);
                                node.release();
                                break;
                            case DELETE:
                                cache.deleteNode(valString);
                                break;
                        }
                        counter++;
                    }
                }
                stopBarrier.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static void singleIteration(int numElements, int numThreads, long seed,
            DataTree tree,
            List<String> values,
            List<Operation> operations,
            double getFraction,
            double putFraction, int iteration,
            int letterCount, boolean warmUp,
            ExecutorService executor)
            throws InterruptedException {

        int[] threadId = new int[numElements];

        for (int i = 0; i < numElements; i++) {
            threadId[i] = i % numThreads;
        }

        CountDownLatch startBarrier = new CountDownLatch(numThreads + 1);
        CountDownLatch stopBarrier = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            TestThread runnable = new TestThread(startBarrier, stopBarrier,
                    i, values, operations, threadId, tree);
            executor.submit(runnable);
        }

        System.gc();

        try {
            long startTime = System.nanoTime();

            startBarrier.countDown();

            stopBarrier.await();

            long endTime = System.nanoTime();

            tree.close();

            Formatter formatter = new Formatter();
            String type;

            if (tree instanceof ConcurrentTree) {
                type = "ConcurrentTree";
            } else if (tree instanceof Tree) {
                type = "Tree";
            } else {
                type = "Unknown";
            }

            double elapsedTime = endTime - startTime;
            double throughput = (numElements / elapsedTime) * 1E9;

            System.out.println(formatter.format(formatString,
                    type, numThreads, throughput, iteration,
                    numElements, getFraction, putFraction, seed, letterCount,
                    warmUp));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void verifyPositive(int target, String description) {
        if (target <= 0) {
            throw new IllegalStateException("The field \"" +
                                            description + "\" must be a positive integer.");
        }
    }

    private static void verifyFraction(double target, String description) {
        if (target < 0.0 || target > 1.0) {
            throw new IllegalStateException("The field \"" +
                                            description + "\" must be between 0.0 and 1.0 (inclusive).");
        }
    }

    private static final String formatString = "%-20s %-10s %-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s";

    public static void main(String[] args) throws Exception {
        int numElements, startThread, stopThread, numIterations, letterCount;
        long seed;
        double getFraction, putFraction;
        Type type = null;

        System.setProperty("eps.gz.type", "0");
        TreeCommonParameters.setDefaultMaxCacheSize(numPagesInMemory);

        if (args.length < 8 || args.length > 9) {
            System.err.println("usage: numElements startThread stopThread numIterations getFraction putFraction seed letters [type]");
            System.exit(-1);
        }

        int currentArg = 0;
        numElements = Integer.parseInt(args[currentArg++]);
        startThread = Integer.parseInt(args[currentArg++]);
        stopThread = Integer.parseInt(args[currentArg++]);
        numIterations = Integer.parseInt(args[currentArg++]);
        getFraction = Double.parseDouble(args[currentArg++]);
        putFraction = Double.parseDouble(args[currentArg++]);
        seed = Long.parseLong(args[currentArg++]);
        letterCount = Integer.parseInt(args[currentArg++]);

        if (args.length == 9) {
            type = Type.valueOf(args[currentArg++].toUpperCase());
        }

        verifyPositive(numElements, "numElements");
        verifyPositive(startThread, "startThread");
        verifyPositive(stopThread, "stopThread");
        verifyPositive(numIterations, "numIterations");
        verifyPositive(letterCount, "letters");
        verifyFraction(getFraction, "getFraction");
        verifyFraction(putFraction, "putFraction");


        if (getFraction + putFraction > 1.0) {
            throw new IllegalStateException("The sum of getFraction and putFraction is > 1.0");
        }

        Formatter formatter = new Formatter();
        System.out.println(formatter.format(formatString,
                "type", "numThreads", "throughput(/sec)", "iteration",
                "elements", "getFr", "putFr", "seed", "letters", "warmup"));

        // warmup the JVM
        runSeveralIterations(10000, 2, 2, 5, seed, getFraction, putFraction, type, letterCount, true);

        // run the tests
        runSeveralIterations(numElements, startThread, stopThread, numIterations, seed, getFraction,
                putFraction, type, letterCount, false);
    }

    private static void runSeveralIterations(int numElements, int startThread, int stopThread, int numIterations,
            long seed, double getFraction,
            double putFraction, Type inputType,
            int letterCount, boolean warmup) throws Exception {
        Iterator<Type> iterator;
        if (inputType == null) {
            iterator = Arrays.asList(Type.values()).iterator();
        } else {
            iterator = Iterators.singletonIterator(inputType);
        }

        ExecutorService executor = Executors.newFixedThreadPool(stopThread);

        File templateDir = makeTemporaryDirectory();
        Random rng = new Random(seed);
        ArrayList<String> values = new ArrayList<>(numElements);
        ArrayList<Operation> operations = new ArrayList<>(numElements);

        populateTree(numElements, getFraction, putFraction, letterCount, templateDir, rng, values, operations);

        File dir = makeTemporaryDirectory();

        FileUtils.copyDirectory(templateDir, dir);

        while (iterator.hasNext()) {
            Type type = iterator.next();
            for (int threadCount = startThread; threadCount <= stopThread; threadCount++) {
                for (int iter = 0; iter < numIterations; iter++) {
                    try {
                        DataTree tree;
                        switch (type) {
                            case CONCURRENT_TREE:
                                tree = new ConcurrentTree.Builder(dir, false).maxCacheSize(numPagesInMemory).build();
                                break;
                            case TREE:
                                tree = new Tree(dir, false, true);
                                break;
                            default:
                                throw new IllegalStateException("Unknown tree type:" + type.toString());
                        }
                        singleIteration(numElements, threadCount, seed, tree,
                                values, operations, getFraction, putFraction, iter,
                                letterCount, warmup, executor);
                    } finally {
                        if (dir != null) {
                            Files.deleteDir(dir);
                        }
                    }
                }
            }
        }

        executor.shutdownNow();

        if (templateDir != null) {
            Files.deleteDir(templateDir);
        }
    }

    private static void populateTree(int numElements, double getFraction, double putFraction,
            int letterCount, File dir, Random rng, ArrayList<String> values,
            ArrayList<Operation> operations) throws Exception {
        for (int i = 0; i < numElements; i++) {
            String nextVal = RandomStringUtils.random(letterCount, 0, 0, false, false, null, rng);
            values.add(nextVal);
        }

        Collections.shuffle(values, rng);

        DataTree tree = new Tree(dir, false, true);


        for (int i = 0; i < numElements; i++) {
            double selectOp = rng.nextDouble();
            if (selectOp < getFraction) {
                DataTreeNode node = tree.getOrCreateNode(values.get(i), null);
                node.release();
                operations.add(Operation.GET);
            } else if (selectOp < getFraction + putFraction) {
                operations.add(Operation.INSERT);
            } else {
                DataTreeNode node = tree.getOrCreateNode(values.get(i), null);
                node.release();
                operations.add(Operation.DELETE);
            }
        }

        tree.close();
    }

}
