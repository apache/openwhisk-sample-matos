package com.messagehub.samples;

import com.google.gson.JsonObject;

public class MessageHubJavaWhiskAction {

    public static JsonObject main(JsonObject args) {

        String broker = "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093",
               rest = "https://kafka-rest-prod01.messagehub.services.us-south.bluemix.net:443",
               apikey = "replaceme";
        if (args.has("broker"))
            broker = args.getAsJsonPrimitive("broker").getAsString();
        if (args.has("rest"))
            rest = args.getAsJsonPrimitive("rest").getAsString();
        if (args.has("apikey"))
            apikey = args.getAsJsonPrimitive("apikey").getAsString();

        String argsStr[] = new String[] {broker, rest, apikey};
        try {
            // save current context class loader, so that it can be restored after the main action class is over
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            // update context class loader to the one that sees the classes of the action
            Thread.currentThread().setContextClassLoader(MessageHubFetcher.class.getClassLoader());

            // invoke "main" action class 
	    MessageHubFetcher.main(argsStr);

            // restore original context class loader, so that it can be used by the java action 'wrapper' code
            Thread.currentThread().setContextClassLoader(cl);
        } catch (Exception e) {
            e.printStackTrace();
        }

        JsonObject response = new JsonObject();
        response.addProperty("lastOffset", MessageHubFetcher.lastOffset);
        return response;

    }
}
