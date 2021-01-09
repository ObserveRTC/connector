//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.observertc.webrtc.connector.adapters;

public interface Task extends Runnable, AutoCloseable{

    Task withTask(Task task, Task... dependencies);

    default String getName() {
        return this.getClass().getSimpleName();
    }

    default String getDescription() {
        return "No Description provided";
    }

    default void close() throws Exception {

    }
}
