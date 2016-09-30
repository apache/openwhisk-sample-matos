#/bin/sh

whisk_create_or_update () {

    ACTION=$1
    FILE=$2
    wsk action get -s $ACTION 2> /dev/null
    if [ $? -eq 0 ]
    then
        echo Updating action $ACTION referring to $FILE
        wsk action update $ACTION $FILE
    else
        echo Creating action $ACTION referring to $FILE
        wsk action create $ACTION $FILE
    fi
}

echo Updating Whisk...
whisk_create_or_update matos/load run/matos-load.jar
whisk_create_or_update matos/batch run/matos-batch.jar
whisk_create_or_update matos/monitor run/matos-monitor.jar
whisk_create_or_update matos/batchW js/batchW.js
echo Done

