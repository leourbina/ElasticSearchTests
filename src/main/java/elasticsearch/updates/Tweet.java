package elasticsearch.updates;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Tweet {
  private final String message;
  private List<Integer> usersRetweeted;

  public Tweet(String message) {
    this.message = message;
    this.usersRetweeted = new ArrayList<>();
  }

  public String getMessage() {
    return message;
  }

  public String getType() {
    return "tweet";
  }

  public String getId() {
    return String.format("tweet-%s", message.hashCode());
  }

  public void retweet(int userId) {
    usersRetweeted.add(userId);
  }

  public XContentBuilder getUpdateRetweetsContentBuilder() throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field("usersRetweeted", usersRetweeted)
        .endObject();
  }

  public XContentBuilder getContentBuilder() throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field("message", message)
        .field("usersRetweeted", usersRetweeted)
        .endObject();
  }
}
