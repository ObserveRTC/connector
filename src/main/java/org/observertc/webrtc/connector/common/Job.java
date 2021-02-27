//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.observertc.webrtc.connector.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class Job implements Task {
    private static final Logger logger = LoggerFactory.getLogger(Job.class);
    private final Map<Task, List<Task>> taskGraph;
    private final String name;
    private String description;
    private volatile boolean run;

    public Job(String name) {
        this.run = false;
        this.taskGraph = new HashMap();
        if (name == null) {
            this.name = this.getClass().getName();
        } else {
            this.name = name;
        }
    }

    public Job() {
        this(null);
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public Job withDescription(String description) {
        this.description = description;
        return this;
    }

    public Task withTask(Task task, Task... dependencies) {
        if (this.taskGraph.containsKey(task)) {
            throw new IllegalStateException("Task " + task.getName() + "has already been added to the job. Adding twice violates the the rule of universe.");
        }

        List<Task> listOfDependencies = Arrays.asList(dependencies);
        this.taskGraph.put(task, listOfDependencies);
        return this;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run() {
        this.perform();
    }

    void perform() {
        this.checkCycles();
        Set<Task> visited = new HashSet();
        Iterator<Task> tasks = this.taskGraph.keySet().stream()
                .map(task -> this.getTopologicalOrder(task, visited))
                .flatMap(Queue::stream).iterator();

        for (; tasks.hasNext();) {
            try (Task task = tasks.next()) {
                task.run();
            }catch (Exception ex) {
                logger.error("Task execution for " + this.name + " is failed", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private Queue<Task> getTopologicalOrder(Task task, Set<Task> visited) {
        Queue<Task> result = new LinkedList();
        if (visited.contains(task)) {
            return result;
        } else {
            visited.add(task);
            List<Task> adjacents = (List)this.taskGraph.get(task);
            if (adjacents != null) {
                Iterator it = adjacents.iterator();

                while(it.hasNext()) {
                    Task adjacent = (Task)it.next();
                    if (!visited.contains(adjacent)) {
                        Queue<Task> toExecute = this.getTopologicalOrder(adjacent, visited);
                        result.addAll(toExecute);
                    }
                }
            }

            result.add(task);
            return result;
        }
    }

    private void checkCycles() {
        Iterator it = this.taskGraph.entrySet().iterator();

        while(it.hasNext()) {
            Entry<Task, List<Task>> entry = (Entry)it.next();
            Task task = (Task)entry.getKey();
            Map<Task, Boolean> recursiveFlags = new HashMap();
            this.checkCycle(task, recursiveFlags);
        }

    }

    private void checkCycle(Task task, Map<Task, Boolean> recursiveFlags) throws IllegalStateException {
        Boolean recursiveFlag = (Boolean)recursiveFlags.get(task);
        if (recursiveFlag != null && recursiveFlag) {
            throw new IllegalStateException("There is a recursive dependency for task: " + task.getName());
        } else {
            recursiveFlags.put(task, true);
            List<Task> adjacents = (List)this.taskGraph.get(task);
            if (adjacents != null) {
                Iterator it = adjacents.iterator();

                while(it.hasNext()) {
                    Task adjacent = (Task)it.next();
                    this.checkCycle(adjacent, recursiveFlags);
                }
            }

            recursiveFlags.put(task, false);
        }
    }
}
