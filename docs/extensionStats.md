Extension Stat Evaluation
===

WebRTC-Reports schema contain an Extension Stat, which is provided
by the application arbitrary passed to the observer, and saved 
in the connected database under Extension table.

Extensions are different from `marker`.
When `marker` is set in observer-js, every report 
will be marked with that string up until marker is changed. 
`ExtensionStat` is setup by a user and send as a separate report. 
There are two fields an extension stat have:
 * `extensionType`
 * `extensionPayload`

To evaluate the extension stat in `connector`, we made an example 
class called `ExtensionStatEvaluator` in the 
`org.observertc.webrtc.connector.transformations` package. 

### Write custom evaluation

Open the class file `org.observertc.webrtc.connector.transformations.ExtensionStatEvaluator`
As this class implements `Transformation` interface, it receives reports 
through the `transform` method. 

Further down the execution flow, if the report type is EXTENSION, 
then it calls the `makeExtensionReport`.
The `makeExtensionReport` method extract payloads from the report 
and calls the `evaluate` method finally got the type and payload of the extension.

Custom evaluation code should start here. Look the following example:

```java
private String evaluate(String type, String payload) {
    // evaluate the extension string accordingly
    switch (type) {
        case "CUSTOM_TYPE_1":
            return "Result of evaluation extension payload has CUSTOM_TYPE_1";
        default:
            return "Evaluated value in string";
    }
}
```

This code snippet checks the type of the extension payload and 
return differently if the type is set to `CUSTOM_TYPE_1`.

By applying your evaluation here, you can evaluate your 
custom stats and return the value you want to write 
into the database bound to this type.

