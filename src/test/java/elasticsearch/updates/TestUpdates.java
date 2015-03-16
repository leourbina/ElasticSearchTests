package elasticsearch.updates;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class TestUpdates extends ElasticsearchIntegrationTest{
  private static final String INDEX = "twitter-1";
  private static final String INDEX_MAPPING = "twitter_mapping.json";
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
    client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute();
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
}
