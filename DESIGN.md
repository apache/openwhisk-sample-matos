## MATOS design and implementation details

***Matos*** is implemented in Java, leveraging [Message Hub code samples](https://github.com/ibm-messaging/message-hub-samples/tree/master/java/message-hub-kafka-ssl) to interact with Message Hub, as well as [Stocator](https://github.com/SparkTC/stocator) (which is based on [JOOS](http://joss.javaswift.org/)) to interact with Object Storage. The main runtime platform of *Matos* is [OpenWhisk](https://new-console.ng.bluemix.net/openwhisk/). *Matos* currently comprises three OpenWhisk actions -- **load**, **monitor** and **batch**.

The **load** action comprises a Kafka producer that securely connects to IBM Message Hub, generates a given number of test messages (on a given topic), and exits (returning the new latest offset). For convenience, the action has a default number of records to produce (1000).

The **batch** action comprises a Java program that performs the following:
* securely connects to IBM Message Hub (to a given topic, under a given consumer id),
* retrieves messages from a given partition, within a given range of offsets (into an internal buffer in memory), 
* securely connects to Object Storage and uploads a file containing the retrieved messages,
* commits the latest offset to Kafka (in the context of the specified consumer id), and
* exits, reporting the range of offsets that have been  processed

For convenience, the **batch** action can be invoked without explicitly specifying the offset range, in which case the batch will start with the last previously committed offset, and will process all the messages until the last one available in the topic-partition when the action is invoked (hence, iterative invocations would result in consequent non-overlapping batches). Alternatively, the offsets (and the respective workload of **batch** actions) can be managed externally, using a tracking mechanism based on something like the **monitor** action below.

The **monitor** action comprises two Kafka consumers. The first consumer securely connects to Message Hub in the context of a given consumer id (typically should be same consumer id as the one used by the **batch** action), and retrieves the last committed offset for the topic-partition. The second consumer connects with its own consumer id, and retrieves the latest available offset for the topic-partition. Then the action exits returning the offsets.

There are several implementation details that had to be adjusted to fit the runtime environment of OpenWhisk, as outlined below.

### Gotcha's
* **Single jar**: the definition of a Java action in OpenWhisk currently supports only passing a single jar. Hence, it was required to package all the dependencies (Kafka client, Stocator, etc) together with Matos classes. This is done in the *oneJar* task in [`build.gradle`](/build.gradle).
* **Multiple actions**: for convenience, the three actions have been developed as part of the same eclipse/gradle project. However, OpenWhisk CLI currently has a built-in logic to determine the main class (by searching the classes in the jar for methods with the predefined signature). While this search is convenient in many cases, in order to create the individual actions we need to produce three different jar files - each with just one class containing the respective OpenWhisk's main method (refer to the respective gradle tasks inheriting from *oneJar* mentioned above). Note that this will not be required once the CLI supports explicit specification of the Main class.
* **Large jar archives**: an additional side-effect of the automatic search for main class within the jar is that it might take very long (i.e., minutes) in case of large jar archives (like in case of Matos, which contains thousands of classes including all the dependnecies). In order to "help" OpenWhisk CLI to find the proper main class quickly, we "re-package" the jars so that the main class is located in the beginning of the archive (this is done in the [`rejar.sh`](/rejar.sh) script). Note that this will not be required once the CLI supports explicit specification of the Main class.
* **Java resources**: while an OpenWhisk action is always packaged in a jar file, some libraries expect to find certain resource files in the filesystem. Hence, in order to simplify and unify access to resource files across different modes of invocation (Eclipse, standalone jar, OpenWhisk action), we generate and/or copy certain resources files to the filesystem (in the [Utils](/src/com/ibm/matos/Utils.java) class). This includes `jaas.conf` (used by the standard JAAS library, for secure connection to Kafka), as well as Kafka client configuration files. Similarly, given that we don't have visibility into the Java installation details in the action's sandbox, we dynamically update the SSL truststore location of the Kafka client to "lib/security/cacerts" path under the folder specified in the JRE's "java.home" property.
* **Object Storage authentication**: the [JOOS](http://joss.javaswift.org/) library we use for interaction with the Object Storage service currently does not support the preferred authentication method used in Bluemix. Hence, we leverage the "Password Scope Access" authentication provider developed as part of [Stocator](https://github.com/SparkTC/stocator).

### Possible future enhancements
* Improve documentation within the code
* Improve robustness: validation of arguments, better error handling, etc
* Enable batch/monitor actions targeting multiple partitions
* Tests
* Feedback loop that would ensure end-to-end delivery guarantees
* Adaptive controller that would trigger `batch` actions in an intelligent manner
