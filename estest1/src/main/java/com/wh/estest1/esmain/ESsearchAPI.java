package com.wh.estest1.esmain;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

public class ESsearchAPI {
	private static Logger _log = Logger.getLogger(ESsearchAPI.class);
	public static void main(String[] args) {
		// 1.初始化client
		Client client = null;
		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			SearchResponse response1 = client.prepareSearch("twitter", "twitter2").setTypes("tweet", "tweet2")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("multi", "test")) // Query
					.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18)) // Filter
					.setFrom(0).setSize(60).setExplain(true).execute().actionGet();
			
			_log.info(response1.getContext());
			// MatchAll on the whole cluster with all default options
			SearchResponse response2 = client.prepareSearch().execute().actionGet();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}	
