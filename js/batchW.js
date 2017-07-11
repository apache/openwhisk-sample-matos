// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements; and to You under the Apache License, Version 2.0.

function main(params) {
    if(!params.owPath || !params.last || !params.committed) {
        return whisk.error();
    }
    BATCH = params.owPath + '/batch';
    THRESHOLD = 0;
    pending = params.last - params.committed;
    if(pending > THRESHOLD) {
        return whisk.invoke({
            name: BATCH,
            parameters: params,
            blocking: true
        })
        .then(function (activation) {
            return activation.result;
        });
    } else {
        return whisk.done();
    }
}

