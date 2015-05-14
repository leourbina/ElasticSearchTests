package elasticsearch;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

@RunWith(RandomizedRunner.class)
@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class ElasticSearchTests extends ElasticsearchIntegrationTest {
  private static final String INDEX = "twitter-1";
  private static final String INDEX_MAPPING = "twitter_mapping.json";
  private static final String PERCOLATOR_TYPE = ".percolator";
  Tweet tweet;

  int userId1 = 1;
  int userId2 = 2;

  Client client;

  @Before
  public void setup() throws IOException {
    client = ElasticsearchIntegrationTest.client();
    tweet = new Tweet("Hello World");
    tweet.retweet(userId1);
    createIndexFromMapping(client, INDEX, INDEX_MAPPING);
  }

  private void createIndexFromMapping(Client client, String index, String mapping) throws IOException {
    client.admin().indices().prepareCreate(index)
        .setSource(Resources.toString(Resources.getResource(mapping), Charsets.UTF_8))
        .execute()
        .actionGet();
    client.admin().cluster().prepareHealth().setWaitForActiveShards(1).execute();
  }

  @Test
  public void itCanIndexAndSearch() throws IOException {
    client.prepareIndex(INDEX, tweet.getType())
        .setId(tweet.getId())
        .setSource(tweet.getContentBuilder())
        .setConsistencyLevel(WriteConsistencyLevel.ONE)
        .execute()
        .actionGet();

    refresh();

    SearchResponse searchResponse = client.prepareSearch(INDEX)
        .setTypes(tweet.getType())
        .setQuery(QueryBuilders.matchQuery("message", "Hello"))
        .execute()
        .actionGet();

    Assertions.assertThat(searchResponse.getHits().totalHits()).isEqualTo(1);

    tweet.retweet(userId2);
    UpdateResponse updateResponse = client.prepareUpdate(INDEX, tweet.getType(), tweet.getId())
        .setConsistencyLevel(WriteConsistencyLevel.ONE)
        .setDoc(tweet.getUpdateRetweetsContentBuilder())
        .execute()
        .actionGet();

    refresh();

    SearchResponse searchResponse2 = client.prepareSearch(INDEX)
        .setTypes(tweet.getType())
        .setQuery(QueryBuilders.matchQuery("message", "Hello"))
        .addFields("message", "usersRetweeted")
        .execute()
        .actionGet();

    Assertions.assertThat(searchResponse2.getHits().totalHits()).isEqualTo(1);
    List<Object> usersRetweeted = searchResponse2.getHits().getAt(0).field("usersRetweeted").getValues();
    Assertions.assertThat(usersRetweeted).hasSize(2);
  }

  @Test
  public void itCanPercolate() throws IOException {
    MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("message", "Hello");

    IndexResponse indexResponse = client.prepareIndex(INDEX, PERCOLATOR_TYPE, "my-test-query")
        .setSource(XContentFactory.jsonBuilder()
            .startObject()
            .field("query", queryBuilder)
            .endObject())
        .setRefresh(true)
        .setConsistencyLevel(WriteConsistencyLevel.ONE)
        .execute().actionGet();
    refresh();

    XContentBuilder doc = XContentFactory.jsonBuilder()
        .startObject()
        .rawField("doc", tweet.getContentBuilder().bytes())
        .endObject();

    PercolateResponse percolateResponse = client.preparePercolate()
        .setIndices(INDEX)
        .setDocumentType(tweet.getType())
        .setSource(doc)
        .execute()
        .actionGet();

    Assertions.assertThat(percolateResponse.getCount()).isEqualTo(1);
  }

  @Test
  public void itCanPercolateWithFilters() throws IOException {
    MatchAllQueryBuilder query1 = QueryBuilders.matchAllQuery();
    MatchAllQueryBuilder query2 = QueryBuilders.matchAllQuery();

    client.prepareIndex(INDEX, PERCOLATOR_TYPE, "query1")
        .setSource(XContentFactory.jsonBuilder()
            .startObject()
            .field("query", query1)
            .field("userId", 1)
            .endObject())
        .setConsistencyLevel(WriteConsistencyLevel.ONE)
        .execute().actionGet();

    client.prepareIndex(INDEX, PERCOLATOR_TYPE, "query2")
        .setSource(XContentFactory.jsonBuilder()
            .startObject()
            .field("query", query1)
            .field("userId", 2)
            .endObject())
        .setConsistencyLevel(WriteConsistencyLevel.ONE)
        .execute().actionGet();


    XContentBuilder doc = XContentFactory.jsonBuilder()
        .startObject()
        .rawField("doc", tweet.getContentBuilder().bytes())
        .endObject();

    PercolateResponse result = client.preparePercolate()
        .setIndices(INDEX)
        .setDocumentType(tweet.getType())
        .setSource(doc)
        .execute().actionGet();

    Assertions.assertThat(result.getCount()).isEqualTo(2);

    PercolateResponse filteredResult1 = client.preparePercolate()
        .setIndices(INDEX)
        .setDocumentType(tweet.getType())
        .setSource(doc)
        .setPercolateFilter(FilterBuilders.termFilter("userId", 1))
        .execute().actionGet();

    Assertions.assertThat(filteredResult1.getCount()).isEqualTo(1);
    Assertions.assertThat(filteredResult1.getMatches()[0].getId().string()).isEqualTo("query1");

    PercolateResponse filteredResult2 = client.preparePercolate()
        .setIndices(INDEX)
        .setDocumentType(tweet.getType())
        .setSource(doc)
        .setPercolateFilter(FilterBuilders.termFilter("userId", 2))
        .execute().actionGet();

    Assertions.assertThat(filteredResult2.getCount()).isEqualTo(1);
    Assertions.assertThat(filteredResult2.getMatches()[0].getId().string()).isEqualTo("query2");

  }
}
