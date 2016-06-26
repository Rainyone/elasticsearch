package com.wh.estest1.esmain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ESmainTest {
	public static Logger _log = Logger.getLogger(ESmainTest.class);

	public static void main(String[] args) {
		// on startup

		Client client = null;
		try {
			// 1.初始化client

			/*
			 * client = TransportClient.builder().build()
			 * .addTransportAddress(new
			 * InetSocketTransportAddress(InetAddress.getByName("localhost"),
			 * 9300));
			 */
			byte[] b = new byte[] { (byte) (127), (byte) (0), (byte) (0), (byte) (1) };
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(b), 9300));

			// 2.初始化client

			/*
			 * Settings settings = Settings.settingsBuilder()
			 * .put("cluster.name", "elasticsearch").build(); client =
			 * TransportClient.builder().settings(settings).build();
			 */

			// Add transport addresses and do something with the client...
			// 3.初始化client
			/*
			 * Settings settings = Settings.settingsBuilder()
			 * .put("client.transport.sniff", true).build(); client =
			 * TransportClient.builder().settings(settings).build();
			 */
			/*
			 * Other transport client level settings include: Parameter
			 * Description client.transport.ignore_cluster_name Set to true to
			 * ignore cluster name validation of connected nodes. (since 0.19.4)
			 * client.transport.ping_timeout The time to wait for a ping
			 * response from a node. Defaults to 5s.
			 * client.transport.nodes_sampler_interval How often to sample /
			 * ping the nodes listed and connected. Defaults to 5s.
			 */
			XContentBuilder builder = jsonBuilder().startObject().field("user", "kimchy").field("postDate", new Date())
					.field("message", "trying out Elasticsearch").endObject();
			/*
			 * If you need to see the generated JSON content, you can use the
			 * string() method.
			 */
			String json = builder.string();
			_log.info("json: " + json);

			IndexResponse response = client.prepareIndex("twitter", "tweet", "1").setSource(builder).get();

			/*
			 * IndexResponse response = client.prepareIndex("twitter", "tweet")
			 * .setSource(json) .get();
			 */
			/*
			 * Note that you can also index your documents as JSON String and
			 * that you don’t have to give an ID:
			 */
			/*
			 * IndexResponse response2 = client.prepareIndex("twitter", "tweet")
			 * .setSource(builder).get();
			 */
			/* IndexResponse object will give you a report: */
			// Index name
			String _index = response.getIndex();
			_log.info("_index: " + _index);
			// Type name
			String _type = response.getType();
			_log.info("_type: " + _type);
			// Document ID (generated or not)
			String _id = response.getId();
			_log.info("_id: " + _id);
			// Version (if it's the first time you index this document, you will
			// get: 1)
			long _version = response.getVersion();
			_log.info("_version: " + _version);
			// isCreated() is true if the document is a new one, false if it has
			// been updated
			boolean created = response.isCreated();
			_log.info("created: " + created);
			// } catch (UnknownHostException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();

			// Get API
			// GetResponse getResponse = client.prepareGet("twitter", "tweet",
			// "1").get();
			/*
			 * operationThreaded is set to true which means the operation is
			 * executed on a different thread. Here is an example that sets it
			 * to false:
			 */
			GetResponse getResponse = client.prepareGet("twitter", "tweet", "1").setOperationThreaded(false).get();
			_log.info("getResponse: " + getResponse.getIndex());

			// Delete API
			// DeleteResponse delResponse = client.prepareDelete("twitter",
			// "tweet", "1").get();

			// Update API
			// You can either create an UpdateRequest and send it to the client:

			/*
			 * UpdateRequest updateRequest = new UpdateRequest();
			 * updateRequest.index("twitter"); updateRequest.type("type");
			 * updateRequest.id("1");
			 * updateRequest.doc(jsonBuilder().startObject().field("user",
			 * "male").endObject()); try {
			 * 
			 * client.update(updateRequest).get();
			 * 
			 * } catch (InterruptedException e) { // TODO Auto-generated catch
			 * block e.printStackTrace(); } catch (ExecutionException e) { //
			 * TODO Auto-generated catch block e.printStackTrace(); } // Or you
			 * can use prepareUpdate() method:
			 * 
			 * client.prepareUpdate("ttl", "doc", "1") .setScript(new Script(
			 * "ctx._source.gender = \"male\"", ScriptService.ScriptType.INLINE,
			 * null, null)) .get();
			 * 
			 * client.prepareUpdate("ttl", "doc", "1")
			 * .setDoc(jsonBuilder().startObject().field("gender",
			 * "male").endObject()).get();
			 */

			/*
			 * The multi get API allows to get a list of documents based on
			 * their index, type and id:
			 */
			MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("twitter", "tweet", "1")
					.add("twitter", "tweet", "2", "3", "4")
					// .add("another", "type", "foo")
					.get();
			_log.info(multiGetItemResponses.contextSize());
			for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
				GetResponse mGetResponse = itemResponse.getResponse();
				_log.info(mGetResponse);
				if (mGetResponse.isExists()) {
					String mGetJson = mGetResponse.getSourceAsString();
					_log.info(mGetJson);
				}
			}
			
			
			
			/*The bulk API allows one to index and delete several documents in a single request. Here is a sample usage:*/
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			// either use client#prepare, or use Requests# to directly build index/delete requests
			bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("user", "kimchy")
			                        .field("postDate", new Date())
			                        .field("message", "trying out Elasticsearch")
			                    .endObject()
			                  )
			        );

			bulkRequest.add(client.prepareIndex("twitter2", "tweet2", "1")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("user", "kimchy2")
			                        .field("postDate", new Date())
			                        .field("message", "another post")
			                    .endObject()
			                  )
			        );
			
			
			/*The BulkProcessor class offers a simple interface to flush bulk operations automatically based on the number or size of requests, or after a given period.*/
			/*By default, BulkProcessor:

				sets bulkActions to 1000
				sets bulkSize to 5mb
				does not set flushInterval
				sets concurrentRequests to 1
				sets backoffPolicy to an exponential backoff with 8 retries and a start delay of 50ms. The total wait time is roughly 5.1 seconds.*/
			BulkResponse bulkResponse = bulkRequest.get();
			if (bulkResponse.hasFailures()) {
			    // process failures by iterating through each bulk response item
			}
			BulkProcessor bulkProcessor = BulkProcessor.builder(
			        client, //Add your elasticsearch client 
			        new BulkProcessor.Listener() {
			            public void beforeBulk(long executionId,
			                                   BulkRequest request) {
							/*
							 * //This method is called just before bulk is
							 * executed. You can for example see the
							 * numberOfActions with
							 */			            	_log.info(request.numberOfActions());
			            } 

			            public void afterBulk(long executionId,
			                                  BulkRequest request,
			                                  BulkResponse response) {
			            	//This method is called after bulk execution. You can for example check if there was some failing requests with 
			            	_log.info(response.hasFailures());
			            } 

			            public void afterBulk(long executionId,
			                                  BulkRequest request,
			                                  Throwable failure) {  
			            	//This method is called when the bulk failed and raised a Throwable
			            } 
			        })
			        .setBulkActions(10000) //We want to execute the bulk every 10 000 requests
			        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) //We want to flush the bulk every 1gb
			        .setFlushInterval(TimeValue.timeValueSeconds(5)) //We want to flush the bulk every 5 seconds whatever the number of requests
			        .setConcurrentRequests(1) //Set the number of concurrent requests. A value of 0 means that only a single request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed while accumulating new bulk requests.
			        .setBackoffPolicy(
			        		/*Set a custom backoff policy which will initially wait for 100ms, 
			        		 * increase exponentially and retries up to three times. A retry 
			        		 * is attempted whenever one or more bulk item requests have failed
			        		 *  with an EsRejectedExecutionException which indicates that there
			        		 *   were too little compute resources available for processing
			        		 *    the request. To disable backoff, pass BackoffPolicy.noBackoff().
			        		 * */
			            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) 
			        .build();
			
			/*Then you can simply add your requests to the BulkProcessor:*/

			// bulkProcessor.add(new IndexRequest("twitter", "tweet",
			// "1").source(/* your doc here */));
			bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));
/*			When all documents are loaded to the BulkProcessor it can be closed by using awaitClose or close methods:*/
				bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
				//bulkProcessor.close();
			/*
			 * Both methods flush any remaining documents and disable all other
			 * scheduled flushes if they were scheduled by setting
			 * flushInterval. If concurrent requests were enabled the awaitClose
			 * method waits for up to the specified timeout for all bulk
			 * requests to complete then returns true, if the specified waiting
			 * time elapses before all bulk requests complete, false is
			 * returned. The close method doesn’t wait for any remaining bulk
			 * requests to complete and exits immediately.
			 */
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// on shutdown
				client.close();
		}
	}
}
