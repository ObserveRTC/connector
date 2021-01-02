package org.observertc.webrtc.reportconnector;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

public class Sleeper implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Sleeper.class);

    public static Sleeper makeFromSystemEnv(String envName, ChronoUnit unit) {
        String initialWaitStr = System.getenv(envName);
        if (initialWaitStr == null) {
            return new Sleeper(() -> 0);
        }
        try {
            int initialWait = Integer.parseInt(initialWaitStr);
            Long initialWaitInMs = Duration.of(initialWait, unit).toMillis();
            return new Sleeper(() -> initialWaitInMs.intValue());
        } catch (Exception ex) {
            logger.error("Error happened parsing " + envName, ex);
            return new Sleeper(() -> 0);
        }

    }

    private final Supplier<Integer> timeInMsProvider;
    private final boolean log;

    public Sleeper(Supplier<Integer> timeInMsProvider) {
        this(timeInMsProvider, false);
    }

    public Sleeper(Supplier<Integer> timeInMsProvider, boolean log) {
        this.timeInMsProvider = timeInMsProvider;
        this.log = log;
    }

    @Override
    public void run() {
        if (timeInMsProvider == null) {
            throw new NullPointerException();
        }
        int initialWaitInMs = this.timeInMsProvider.get();
        if (this.log) {
            logger.info("The configured sleeping time is {}ms", initialWaitInMs);
        }

        if (initialWaitInMs < 1) {
            if (initialWaitInMs < 0) {
                logger.info("The configured initial waiting time ({} ms) is less than 0", initialWaitInMs);
            }
            return;
        }

        try {
            Thread.sleep(initialWaitInMs);
        } catch (InterruptedException e) {
            logger.error("Error happened in waiting ", e);
        }
    }
}

