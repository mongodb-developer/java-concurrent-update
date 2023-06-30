# java-concurrent-update

  Sample code showing how to use the MongoDB Java drivers to make updates to
  documents in an environment where there is potential for simultaneous updates
  by multiple clients to overwrite one another.

  We update the documents using updateMany in three phases:
        
  Firstly we try to set a lock on the documents to be updated by adding
  our process ID to the "lockStatus" field on each document to be updated,
  but only if the current value of the "lockStatus" field is set to "unlocked".
  As part of this first stage we also add a timestamp to show when the lock was set.
  
  Next, we look at the result object returned by the initial "lock" update and verify
  the number of documents that were updated matches what we were expecting. If it is
  less than what we were expecting, it would indicate that another process has one
  or more documents locked, and we should try again, possibly with an appropriate wait
  period first.
  
  If the number of documents marked as locked matches what we expected, we can then go
  ahead and make our updates. Depending on the nature of the updates and how you calculate
  the updated values, you might need to retrieve the documents first to ensure you have
  the latest version of any fields used to calculate the update values.
  
  Finally, we do a final update to unlock the documents. We do this in the 'finally'
  clause of a try block to make sure the unlock happens even in the event of an
  exception occurring.
  
  This process is reliant on the fact that an update to a document in MongoDB is always
  ACIDic. Note also that if an exception is thrown by the UpdateMany command, none of
  the matching documents are updated.
  
  Using the processID of the locking process and adding a lockTimestamp will allow you
  to add a cleanup process to handle a client process dying before removing its locks. The
  cleanup process should periodically look for documents locked by a process that no
  longer exists, or has been locked for longer than a reasonable timeout value.

  For this example, we assume we're working with a collection called UpdateExamples
  containing the following documents:
  
      {"_id": 1, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
      {"_id": 2, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
      {"_id": 3, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
      {"_id": 4, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" },
      {"_id": 5, "lockStatus": "unlocked", "lockTS": null, "dataPayload": "" }
        
