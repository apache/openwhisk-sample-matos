#/bin/sh
echo Building jars...
gradle jarMonitor jarLoader jarBatch

echo -n Repackaging jars...

rm -rf tmp/* run/matos-load.jar ; cd tmp ; jar xf ../build/libs/matos-load-1.0.jar ; jar cfe ../run/matos-load.jar com.ibm.matos.Load com/ibm/matos/Load.class *; cd ..
echo -n Load..

rm -rf tmp/* run/matos-batch.jar ; cd tmp ; jar xf ../build/libs/matos-batch-1.0.jar ; jar cfe ../run/matos-batch.jar com.ibm.matos.Batch com/ibm/matos/Batch.class *; cd ..
echo -n Batch..

rm -rf tmp/* run/matos-monitor.jar ; cd tmp ; jar xf ../build/libs/matos-monitor-1.0.jar ; jar cfe ../run/matos-monitor.jar com.ibm.matos.Monitor com/ibm/matos/Monitor.class *; cd ..
echo Monitor

echo Done
