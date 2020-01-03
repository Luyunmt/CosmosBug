
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.azure.data.cosmos.ChangeFeedProcessor;
import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosClientException;
import com.azure.data.cosmos.CosmosContainer;
import com.azure.data.cosmos.CosmosContainerProperties;
import com.azure.data.cosmos.CosmosContainerRequestOptions;
import com.azure.data.cosmos.CosmosContainerResponse;
import com.azure.data.cosmos.CosmosDatabase;
import com.azure.data.cosmos.CosmosItemProperties;
import com.azure.data.cosmos.SerializationFormattingPolicy;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.commons.lang3.RandomStringUtils;


import rx.Observable;

public class Test {
    private static boolean isWorkCompleted = false;
    public static int WAIT_FOR_WORK = 900;
    private static AsyncDocumentClient asyncDocumentClient;

    public static void main(String[] args) throws InterruptedException {
        try {
            setUp();
            com.azure.data.cosmos.ConnectionPolicy connectionPolicy = new com.azure.data.cosmos.ConnectionPolicy();
            connectionPolicy.connectionMode(com.azure.data.cosmos.ConnectionMode.DIRECT);
            CosmosClient client = CosmosClient.builder().connectionPolicy(connectionPolicy)
                    .endpoint("{COSMOSENDPOINT}")
                    .key("{COSMOSKEY}")
                    .build();

            CosmosContainer feedContainer = createNewCollection(client, "11111", "aaaa");
            CosmosContainer leaseContainer = createNewLeaseCollection(client, "11111", "aaaa" + "-leases");
            createDocument_Async();
            ChangeFeedProcessor changeFeedProcessorInstance = getChangeFeedProcessor("SampleHost_1", feedContainer,
                    leaseContainer);

            changeFeedProcessorInstance.start().subscribe(aVoid -> {
                createNewDocuments(feedContainer, 10, Duration.ofSeconds(3));
                isWorkCompleted = true;
            });
            // createDocument_Async(); this method using package azure-cosmosdb 2.6.3
            createNewDocuments(feedContainer, 1, Duration.ofSeconds(3)); // this method using package azure-cosmos 3.4.0
            long remainingWork = WAIT_FOR_WORK;
            while (!isWorkCompleted && remainingWork > 0) {
                Thread.sleep(1000);
                remainingWork -= 100;
            }
            if (isWorkCompleted) {
                if (changeFeedProcessorInstance != null) {
                    changeFeedProcessorInstance.stop().subscribe().wait(10000);
                }
            } else {
                throw new RuntimeException(
                        "The change feed processor initialization and automatic create document feeding process did not complete in the expected time");
            }
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("END Sample");
        System.exit(0);
    }

    public static void setUp() throws InterruptedException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);
        asyncDocumentClient = new AsyncDocumentClient.Builder()
                .withServiceEndpoint("{COSMOSENDPOINT}")
                .withMasterKeyOrResourceToken(
                        "{COSMOSKEY}")
                .withConnectionPolicy(connectionPolicy).withConsistencyLevel(ConsistencyLevel.Session).build();

        Database databaseDefinition = new Database();

        databaseDefinition.setId("11111");
        // create the database

        Observable<ResourceResponse<Database>> createDatabaseObservable = asyncDocumentClient
                .createDatabase(databaseDefinition, null);
        final CountDownLatch completionLatch = new CountDownLatch(1);
        createDatabaseObservable.single() // We know there is only single result
                .subscribe(databaseResourceResponse -> {
                    System.out.println(databaseResourceResponse.getActivityId());
                    completionLatch.countDown();
                }, error -> {
                    System.err.println(
                            "an error occurred while creating the database: actual cause: " + error.getMessage());
                    completionLatch.countDown();
                });
        completionLatch.await();

    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosContainer feedContainer,
            CosmosContainer leaseContainer) {

        return ChangeFeedProcessor.Builder().hostName(hostName).feedContainer(feedContainer)
                .leaseContainer(leaseContainer).handleChanges(docs -> {
                    System.out.println("--->handleChanges() START");
                    for (CosmosItemProperties document : docs) {
                        System.out.println(
                                "---->DOCUMENT RECEIVED: " + document.toJson(SerializationFormattingPolicy.INDENTED));
                    }
                    System.out.println("--->handleChanges() END");
                })

                .build();

    }

    public static CosmosContainer createNewCollection(CosmosClient client, String databaseName, String collectionName) {
        CosmosDatabase databaseLink = client.getDatabase(databaseName);
        CosmosContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosContainerResponse containerResponse = null;
        try {
            containerResponse = collectionLink.read().block();
            if (containerResponse != null) {
                throw new IllegalArgumentException(
                        String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex.getCause();
                if (cosmosClientException.statusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }
        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        containerResponse = databaseLink.createContainer(containerSettings, 10000, requestOptions).block();
        if (containerResponse == null) {
            throw new RuntimeException(
                    String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }
        return containerResponse.container();
    }

    public static CosmosContainer createNewLeaseCollection(CosmosClient client, String databaseName,
            String leaseCollectionName) {

        CosmosDatabase databaseLink = client.getDatabase(databaseName);
        CosmosContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;
        try {
            leaseContainerResponse = leaseCollectionLink.read().block();
            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex.getCause();
                if (cosmosClientException.statusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }
        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        leaseContainerResponse = databaseLink.createContainer(containerSettings, 400, requestOptions).block();
        if (leaseContainerResponse == null) {
            throw new RuntimeException(
                    String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }
        return leaseContainerResponse.container();

    }

    public static void createNewDocuments(CosmosContainer containerClient, int count, Duration delay) {
        String suffix = RandomStringUtils.randomAlphabetic(10);
        for (int i = 0; i <= count; i++) {
            CosmosItemProperties document = new CosmosItemProperties();
            document.id(String.format("0%d-%s", i, suffix));
            containerClient.createItem(document).subscribe(doc -> {
                System.out.println(
                        "---->DOCUMENT WRITE: " + doc.properties().toJson(SerializationFormattingPolicy.INDENTED));
            });
            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }

        }

    }

    public static void createDocument_Async() throws Exception {

        Document doc = new Document(
                String.format("{ 'id': 'doc%s', 'counter': '%d'}", UUID.randomUUID().toString(), 1));
        Observable<ResourceResponse<Document>> createDocumentObservable = asyncDocumentClient
                .createDocument(getCollectionLink(), doc, null, false);
        final CountDownLatch completionLatch = new CountDownLatch(1);
        // Subscribe to Document resource response emitted by the observable
        createDocumentObservable.single() // We know there will be one response
                .subscribe(documentResourceResponse -> {
                    System.out.println(documentResourceResponse.getActivityId());
                    completionLatch.countDown();
                }, error -> {
                    System.err.println(
                            "an error occurred while creating the document: actual cause: " + error.getMessage());
                    completionLatch.countDown();
                });
        // Wait till document creation completes
        completionLatch.await();
    }

    private static String getCollectionLink() {

        return "dbs/" + "11111" + "/colls/" + "aaaa";

    }
}