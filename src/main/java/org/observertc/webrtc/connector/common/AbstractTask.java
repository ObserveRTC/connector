//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.observertc.webrtc.connector.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTask implements Task {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);
    private String name;
    private String description;
    private volatile boolean run;

    public AbstractTask(String name) {
        this.run = false;
        if (name == null) {
            this.name = this.getClass().getName();
        } else {
            this.name = name;
        }
    }

    @Override
    public Task withTask(Task task, Task... dependencies) {
        throw new IllegalStateException("Abstract task cannot have dependencies. Sorry!");
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public AbstractTask withDescription(String description) {
        this.description = description;
        return this;
    }

    public AbstractTask withName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public void run() {
        if (this.run) {
            logger.warn("Something wrong. A task should not be tried to execute twice.");
            return;
        }
        this.execute();
        this.run = true;
    }

    protected abstract void execute();

}
