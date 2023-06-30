package com.mongodb.devrel.pods.concurrent_update;

import static com.mongodb.client.model.Filters.*;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonNumber;
import org.bson.Document;
import org.bson.BsonInt32;
import org.bson.conversions.Bson;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import java.util.ArrayList;
import java.util.List;

public class ConcurrentUpdate {
    public static void main(String[] args) {

        //We update the documents using updateMany in three phases:
        //
        //Firstly we try to set a lock on the documents to be updated by adding
        //our process ID to the "lockStatus" field on each document to be updated,
        //but only if the current value of the "lockStatus" field is set to "unlocked".
        //As part of this first stage we also add a timestamp to show when the lock was set.
        //
        //Next, we look at the result object returned by the initial "lock" update and verify
        //the number of documents that were updated matches what we were expecting. If it is
        //less than what we were expecting, it would indicate that another process has one
        //or more documents locked, and we should try again, possibly with an appropriate wait
        //period first.
        //
        //If the number of documents marked as locked matches what we expected, we can then go
        //ahead and make our updates. Depending on the nature of the updates and how you calculate
        //the updated values, you might need to retrieve the documents first to ensure you have
        //the latest version of any fields used to calculate the update values.
        //
        //Finally, we do a final update to unlock the documents. We do this in the 'finally'
        //clause of a try block to make sure the unlock happens even in the event of an
        //exception occurring.
        //
        //This process is reliant on the fact that an update to a document in MongoDB is always
        //ACIDic. Note also that if an exception is thrown by the UpdateMany command, none of
        //the matching documents are updated.
        //
        //Using the processID of the locking process and adding a lockTimestamp will allow you
        //to add a cleanup process to handle a client process dying before removing its locks. The
        //cleanup process should periodically look for documents locked by a process that no
        //longer exists, or has been locked for longer than a reasonable timeout value.

        //For this example, we assume we're working with a collection called UpdateExamples
        //containing the following documents:
        /*
            {"_id": 1, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
            {"_id": 2, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
            {"_id": 3, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
            {"_id": 4, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
            {"_id": 5, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" }
        */

        //Replace with your connection string
        String uri = "mongodb://localhost:27017/";

        try (MongoClient mongoClient = MongoClients.create(uri)) {

            MongoDatabase database = mongoClient.getDatabase("Indeed");
            MongoCollection<Document> collection = database.getCollection("UpdateExamples");

            //Create a list of document ID's we want to update
            BsonInt32[] updateIDs = {(new BsonInt32(1)), (new BsonInt32(3)), (new BsonInt32(5))};

            //Get the process ID - note this requires Java 9 or later and might not work in all
            //environments - swap this for a suitable alternative for your environment if necessary.
            // Anything uniquely identifying the calling process will work
            String pid = String.valueOf(ProcessHandle.current().pid());

            boolean updated = false;
            while(!updated) {
                Bson query = and(eq("lockStatus", "unlocked"), in("_id", updateIDs));
                Bson updates = Updates.combine(
                        Updates.set("lockStatus", pid),
                        Updates.currentTimestamp("lockTS"));
                try {
                    UpdateResult result = collection.updateMany(query, updates);
                    System.out.println("Locked document count: " + result.getModifiedCount());
                    if (result.getModifiedCount() == updateIDs.length) {
                        //We've locked the expected number of documents so now make the payload
                        //updates. We're showing an example of using BulkWrite to do multiple
                        //UpdateMany operations in one call to the database
                        List<WriteModel<Document>> bulkOperations = new ArrayList<>();
                        for(BsonInt32 id : updateIDs) {
                            query = and(eq("lockStatus", pid), eq("_id", id));
                            updates = Updates.set("dataPayload", "Record " + String.valueOf(id.intValue()) + " was updated by process:" + pid);
                            UpdateManyModel<Document> payloadDoc = new UpdateManyModel<>(query, updates);
                            bulkOperations.add(payloadDoc);
                        }
                        BulkWriteResult bulkResult = collection.bulkWrite(bulkOperations);
                        System.out.println("Updated document count: " + bulkResult.getModifiedCount());
                        if (bulkResult.getModifiedCount() != updateIDs.length) {
                            //This shouldn't happen, but you may want to think about how to handle it
                            //just in case
                        }
                        updated = true;
                    }
                } catch (Exception me) {
                    System.err.println("Unable to update due to an error: " + me);
                } finally {
                    //unlock the documents
                    query = and(eq("lockStatus", pid), in("_id", updateIDs));
                    updates = Updates.combine(
                            Updates.set("lockStatus", "unlocked"),
                            Updates.set("lockTS", null));
                    collection.updateMany(query, updates);
                }
                if(!updated){
                    wait(500);
                }
            }
        }
    }

    public static void wait(int ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }

}