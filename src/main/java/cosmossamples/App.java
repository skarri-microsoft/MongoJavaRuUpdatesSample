package cosmossamples;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * Hello world!
 *
 */
public class App 
{
    private static MongoClient mongoClient;
    public static void main( String[] args )
    {


         mongoClient=new MongoClient(
                new MongoClientURI(
                        "mongodb://username:password@username.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));

         // Shared DB Operations

         String shareDb="shareDb";

         CreateDatabase(shareDb,2000);

         ShowDatabaseRus(shareDb);

         String coll1="coll1";
         String shardKey="test";

         CreateShardCollection(shareDb,coll1,shardKey);

         mongoClient.getDatabase(shareDb).getCollection(coll1).insertOne(new Document(shardKey, "sample"));

         UpdateDatabaseRus(shareDb,3000);

        ShowDatabaseRus(shareDb);

         // drop the shared database collection
         mongoClient.getDatabase(shareDb).getCollection(coll1).drop();

         DeleteDatabase(shareDb);

         String nonSharedDb="nonshareddb";
         String dedicatedColl1="dedicatedColl1";
         String dedicatedCollShardKey="test";

         mongoClient.getDatabase(nonSharedDb).getCollection(dedicatedColl1).insertOne(new Document(shardKey, "sample"));

         ShowCollectionRus(nonSharedDb,dedicatedColl1);

         UpdateCollectionRus(nonSharedDb,dedicatedColl1,2000);

         ShowCollectionRus(nonSharedDb,dedicatedColl1);

         mongoClient.getDatabase(nonSharedDb).drop();
       }

    public static void CreateShardCollection(String dbName,String collectionName,String shardKey)
    {

        Document cmd = new Document("shardCollection", String.format("%s.%s",dbName,collectionName)).
                append("key", new Document(shardKey,"hashed"));
        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    public static void UpdateDatabaseRus(String dbName,long rus)
    {
        Document cmd = new Document("customAction", "UpdateDatabase").
                append("offerThroughput", rus);

        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);
    }

    public static void UpdateCollectionRus(String dbName,String collName,int rus)
    {
        Document cmd = new Document("customAction", "UpdateCollection").
                append("collection", collName).append("offerThroughput", rus);
        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    public static void ShowDatabaseRus(String dbName)
    {
        Document cmd = new Document("customAction", "GetDatabase");
        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);
    }

    public static void ShowCollectionRus(String dbName,String collName)
    {
        Document cmd = new Document("customAction", "GetCollection").
                append("collection", collName);
        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    public static void CreateDatabase(String dbName,long rus)
    {

        Document cmd = new Document("customAction", "CreateDatabase").
                append("offerThroughput", rus);

        Document output = mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    public static void DeleteDatabase(String dbName)
    {

      mongoClient.getDatabase(dbName).drop();


    }
}
