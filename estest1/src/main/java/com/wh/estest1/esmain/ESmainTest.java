package com.wh.estest1.esmain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ESmainTest {
	public static void main(String[] args) {
		// on startup

		Client client = null;
		try {
			// 1.初始化client
			/*
			 * client = TransportClient.builder().build()
			 * .addTransportAddress(new
			 * InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
			 * .addTransportAddress(new
			 * InetSocketTransportAddress(InetAddress.getByName("host2"),
			 * 9300));
			 */
			// 2.初始化client
			/*
			 * Settings settings = Settings.settingsBuilder()
			 * .put("cluster.name", "myClusterName").build(); client =
			 * TransportClient.builder().settings(settings).build();
			 */
			// Add transport addresses and do something with the client...
			// 3.初始化client
			/*Settings settings = Settings.settingsBuilder()
					.put("client.transport.sniff", true).build();
			client = TransportClient.builder().settings(settings).build();*/
			/*
			 * Other transport client level settings include: Parameter
			 * Description client.transport.ignore_cluster_name Set to true to
			 * ignore cluster name validation of connected nodes. (since 0.19.4)
			 * client.transport.ping_timeout The time to wait for a ping
			 * response from a node. Defaults to 5s.
			 * client.transport.nodes_sampler_interval How often to sample /
			 * ping the nodes listed and connected. Defaults to 5s.
			 */
			XContentBuilder builder = jsonBuilder().startObject()
					.field("user", "kimchy").field("postDate", new Date())
					.field("message", "trying out Elasticsearch").endObject();
			/*
			 * If you need to see the generated JSON content, you can use the
			 * string() method.
			 */
			String json = builder.string();
			System.out.println("json: " + json);

			IndexResponse response = client
					.prepareIndex("twitter", "tweet", "1").setSource(builder)
					.get();
			/*
			 * Note that you can also index your documents as JSON String and
			 * that you don’t have to give an ID:
			 */
			IndexResponse response2 = client.prepareIndex("twitter", "tweet")
					.setSource(builder).get();
			/* IndexResponse object will give you a report: */
			// Index name
			String _index = response.getIndex();
			// Type name
			String _type = response.getType();
			// Document ID (generated or not)
			String _id = response.getId();
			// Version (if it's the first time you index this document, you will
			// get: 1)
			long _version = response.getVersion();
			// isCreated() is true if the document is a new one, false if it has
			// been updated
			boolean created = response.isCreated();
			// } catch (UnknownHostException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
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
