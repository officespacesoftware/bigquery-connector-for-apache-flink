package io.aiven.flink.connectors.bigquery.sink;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

@PublicEvolving
public class BigQuerySink implements Sink<ByteString> {
  protected BigQueryConnectionOptions options;

  public BigQuerySink(BigQueryConnectionOptions options) {
    this.options = options;
  }

  @Override
  public SinkWriter<ByteString> createWriter(InitContext context) throws IOException {
    if (options.getDeliveryGuarantee() != DeliveryGuarantee.AT_LEAST_ONCE) {
      throw new IllegalArgumentException(
          "Unsupported delivery guarantee: " + options.getDeliveryGuarantee());
    }

    return new BigQueryStreamingAtLeastOnceSinkWriter(options);
  }
}
