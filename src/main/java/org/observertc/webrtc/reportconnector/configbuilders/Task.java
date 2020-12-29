//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.observertc.webrtc.reportconnector.configbuilders;

import java.util.Map;

public interface Task {
    String getName();

    String getDescription();

    void execute(Map<String, Map<String, Object>> var1);

    Map<String, Object> getResults();
}
