package com.wh.estest1.esmain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// on shutdown
			if (client != null)
				client.close();
		}
	}
}
