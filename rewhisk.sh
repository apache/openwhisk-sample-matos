#/bin/sh

whisk_action_create_or_update () {

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
wsk package get -s matos 2> /dev/null
if [ $? -ne 0 ]
then
    echo Creating \"matos\" package
    wsk package create matos
fi
whisk_action_create_or_update matos/load run/matos-load.jar
whisk_action_create_or_update matos/batch run/matos-batch.jar
whisk_action_create_or_update matos/monitor run/matos-monitor.jar
whisk_action_create_or_update matos/batchW js/batchW.js
echo Done

