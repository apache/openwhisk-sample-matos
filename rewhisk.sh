#/bin/sh

echo Updating Whisk...
wsk action update matos/load run/matos-load.jar
wsk action update matos/batch run/matos-batch.jar
wsk action update matos/monitor run/matos-monitor.jar

echo Done
