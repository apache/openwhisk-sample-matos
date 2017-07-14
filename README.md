## Serverless **M**essage **A**rchiver **T**o **O**bject **S**torage (MATOS) Sample Application

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
#[![Build Status](https://travis-ci.org/apache/incubator-openwhisk-sample-matos.svg?branch=master)](https://travis-ci.org/apache/incubator-openwhisk-sample-matos)

### Motivation

***Matos*** demonstrates Bluemix-based serverless implementation of a simple pipeline (hosted on OpenWhisk) that reads messages from a Message Hub topic and archives them in batches into an Object Storage folder.

The serverless architecture introduces multiple advantages. First, by leveraging OpenWhisk and given the persistent nature of Message Hub, it is possible to apply the archiving in batches, and pay only for the short periods of execution time (typically seconds) of each batch. Moreover, the architecture can seamlessly accommodate spikes in load due to inherent elasticity of OpenWhisk. The combination of the two can dramatically reduce the overall cost, and increase the elasticity of the solution.

*Matos* is implemented in Java.

***Disclamer***: notice that this implementation is for education/demonstration purposes ONLY. It does not address many important requirements, such as high availability, consistency and delivery guarantees. Moreover, there are several known cases when the program does not function properly (e.g., illegal parameters, invocation 'corner cases', etc).

***Matos*** was inspired by [Secor](https://github.com/pinterest/secor).

### Overview
The heart of ***matos*** is an OpenWhisk action called **batch**, that copies a batch of messages from a Message Hub topic into an Object Storage folder. The action can be invoked passing a range of Kafka message offsets to archive. Otherwise it would archive all the pending messages since the last invocation (enabling, for example, periodic time-based invocation). Every batch will be saved in a separate file within the specified folder, using a naming convention that contains current timestamp and the range of offsets. In addition, there are two helper functions -- **load**, to produce a given amount of test messages into Message Hub, and **monitor**, to retrieve the current offsets (latest and last committed) for a given topic and consumer ID.

The actions receive arguments either via a configuration file (in JSON format, packaged with the action's jar file) or via regular action parameters mechanisms (explicit or package-bound). The parameters are:
* `kafkaBroker` - the address of the Message Hub broker to connect to, as appears in VCAP_SERVICES (e.g., `kafka01-prod01.messagehub.services.us-south.bluemix.net:9093`)
* `kafkaApiKey` (\*) - the API key of your Message Hub instance that you want *Matos* to work with, as appears in VCAP_SERVICES (typically 48 characters)
* `kafkaTopic` - the name of the topic in the above instance (e.g., `matos-topic`)
* `kafkaPartition` - the partition number within the topic (typically 0, unless the topic has multiple partitions)
* `kafkaConsumerId` - a string identifying this Matos instance (e.g., `matos-id`)
* `kafkaStartOffset` - only for **batch** action ("-1" means that last committed offset will be used)
* `kafkaEndOffset` - only for **batch** action ("-1" means that latest available offset will be used)
* `kafkaNumRecords` - only for **load** (e.g., "1000")
* `swiftAuthUrl` - the auth URL of the Object Storage service, as appears in VCAP_SERVICES, with the proper suffix (e.g., `https://identity.open.softlayer.com/v3/auth/tokens`)
* `swiftRegion` - the region of the Object Storage service, as appears in VCAP_SERVICES (e.g., `dallas`)
* `swiftTenantId` (\*) - the tenant ID of the Object Storage service instance you want to work with, as appears in VCAP_SERVICES (typically 32 characters)
* `swiftUserId` (\*) - the user ID of the Object Storage service instance, as appears in VCAP_SERVICES (typically 32 characters)
* `swiftPassword` (\*) - the password for the Object Storage service instance, as appears in VCAP_SERVICES (typically 16 characters. Notice that it may contain special characters, so make sure to escape them with '\')
* `swiftContainer` - the name of the folder within the Object Storage service instance (e.g., `matos-folder`)

See [resources/matos.json](resources/matos.json) for an example. Notice that due to security reasons, parameters marked with (\*) would typically not be specified in the configuration file, but rather as runtime parameters of the OpenWhisk package binding (as demonstrated below).

For convenience, the actions can be also invoked locally as regular Java programs (in which case the config file is passed as first argument, followed by relevant parameters marked above with (\*) - just run them without parameters to see the exact usage). When invoked locally, the `monitor` program runs continuously, retrieving and displaying offsets every 5 seconds (you can interrupt it with Ctrl-C).

### Prerequisites
* Bluemix account
* Message Hub instance
  - You will need to create a **topic** (e.g., `matos-topic`, typically with a single partition), to decide on **consumerId** (arbitrary string - e.g., `matos-id`) and to locate (in VCAP_SERVICES) the **api key** and the address of the **broker**.
* Object Storage instance
  - You will need to create a **folder** (e.g., `matos-folder`) and to locate (in VCAP_SERVICES) the **auth_url**, **region**, **projectId**, **userId** and **password**. The id's are 32 characters long, and the password is 16 characters. Notice that the password may include special characters, which you may need to escape with '\' when specifying in config file and/or command line.
* Local installation and configuration of git, Python, Java 8, gradle, openwhisk CLI (https://new-console.ng.bluemix.net/openwhisk/cli)

### Download and configure:
```sh
:~$ git clone <URL_OF_THIS_REPOSITORY.git>
:~$ cd matos
:~/matos$ mkdir run; mkdir tmp
:~/matos$ vi resources/matos.json
```
Make sure your `matos.json` contains the proper values for `kafkaBroker`, `kafkaTopic`, `kafkaPartition`, `swiftAuthUrl`, `swiftRegion`, `swiftContainer`.

### Build:
```sh
:~/matos$ ./rejar.sh
```
### OpenWhisk configuration:
```sh
:~/matos$ wsk package create matos
:~/matos$ wsk action create matos/load run/matos-load.jar
:~/matos$ wsk action create matos/monitor run/matos-monitor.jar
:~/matos$ wsk action create matos/batch run/matos-batch.jar
:~/matos$ wsk action create matos/batchW js/batchW.js
:~/matos$ wsk package bind matos mymatos --param kafkaApiKey <API_KEY> --param swiftTenantId <TENANT_ID> --param swiftUserId <USER_ID> --param swiftPassword <PASSWORD> --param owPath <OPENWHISK_NAMESPACE>/mymatos
```
### Consequent updates (if you make code changes)
```sh
:~/matos$ ./rejar.sh
:~/matos$ ./rewhisk.sh
```
In order to work with the project in Eclipse, run `gradle eclipse` to download dependencies and to configure the buildpath properly.

Notice that if you want to change some of the OpenWhisk parameters associated with mymatos, you currently need to specify them all in `wsk package update mymatos ...`.

### Example usage
Run the following command in a separate window to continuously observe OpenWhisk logs:
```sh
:~/matos$ wsk activation poll
```
Load some messages into Message Hub (make sure you run the actions under the `mymatos` namespace, where you defined all the credentials):
```sh
:~/matos$ wsk action invoke mymatos/load --blocking --result
{
  "last": "1000"
}
:~/matos$ wsk action invoke mymatos/load --blocking --result
{
  "last": "2000"
}
```
Archive some messages to Object Storage (explicitly specifying offsets):
```sh
:~/matos$ wsk action invoke mymatos/batch --blocking --result --param kafkaStartOffset 0 --param kafkaEndOffset 1000
{
  "last": "[0..1000]"
}
```
Load some more messages:
```sh
:~/matos$ wsk action invoke mymatos/load --blocking --result --param kafkaNumRecords 2000
{
  "last": "4000"
}
```
Check offsets:
```sh
:~/matos$ wsk action invoke mymatos/monitor --blocking --result
{
  "committed": "1000",
  "last": "4000"
}
```
Archive all the pending messages to Object Storage:
```sh
:~/matos$ wsk action invoke mymatos/batch --blocking --result
{
  "last": "[1000..4000]"
}
```
Load more messages:
```sh
:~/matos$ wsk action invoke mymatos/load --blocking --result --param kafkaNumRecords 3000
{
  "last": "7000"
}
:~/matos$ wsk action invoke mymatos/load --blocking --result --param kafkaNumRecords 3000
{
  "last": "10000"
}
```
Archive all the pending messages to Object Storage:
```sh
:~/matos$ wsk action invoke mymatos/batch --blocking --result
{
  "last": "[4000..10000]"
}
```

Alternatively, `monitor` and `batch` could be invoked as part of a sequence, triggered by a timer every 5 minutes:
```sh
:~/matos$ wsk action create matosMB --sequence mymatos/monitor,mymatos/batchW
:~/matos$ wsk trigger create everyFiveMinutes --feed /whisk.system/alarms/alarm -p cron '*/5 * * * *'
:~/matos$ wsk rule create --enable matosEvery5min everyFiveMinutes matosMB
```

As a result, consequent invocations of `load` will be handled in batches every 5 minutes - while `batch` action will be invoked only if new data is available.


Notice that if your Kafka topic has multiple partitions, make sure that you specify the right partition when invoking `batch` or `monitor` actions (0 in the default configuration file).

At any point, you can access the Object Storage service instance to observe the files created for each batch.

### Design and Implementation Details
Refer to [DESIGN.md](DESIGN.md)
