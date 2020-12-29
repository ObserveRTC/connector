//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.observertc.webrtc.reportconnector.configbuilders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

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
        this((String)null);
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

    public void execute(Map<String, Map<String, Object>> results) {
        this.perform();
    }

    public Map<String, Object> getResults() {
        return new HashMap();
    }

    public Job withTask(Task task, Task... dependencies) {
        if (this.taskGraph.containsKey(task)) {
            throw new IllegalStateException("Task " + task.getName() + "has already been added to the job. Adding twice violates the the rule of universe.");
        } else {
            List<Task> listOfDependencies = Arrays.asList(dependencies);
            this.taskGraph.put(task, listOfDependencies);
            return this;
        }
    }

    public void perform() {
        this.checkCycles();
        Set<Task> visited = new HashSet();
        Queue<Task> tasksToExecute = new LinkedList();
        Stream<Queue<Task>> var10000 = this.taskGraph.keySet().stream().map((t) -> {
            return this.getTopologicalOrder(t, visited);
        });
        Objects.requireNonNull(tasksToExecute);
        var10000.forEach(tasksToExecute::addAll);
        while(0 < tasksToExecute.size()) {
            Task task = (Task)tasksToExecute.poll();
            Map<String, Map<String, Object>> results = new HashMap();
            List<Task> adjacents = (List)this.taskGraph.get(task);
            if (adjacents != null) {
                Iterator it = adjacents.iterator();

                while(it.hasNext()) {
                    Task adjacent = (Task)it.next();
                    Map<String, Object> adjacentResults = adjacent.getResults();
                    results.put(adjacent.getName(), adjacentResults);
                }
            }

            logger.info("Executing task {}. Description: {}", task.getName(), task.getDescription());
            task.execute(results);
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
