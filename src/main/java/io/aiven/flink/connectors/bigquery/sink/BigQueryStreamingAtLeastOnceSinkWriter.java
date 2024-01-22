package io.aiven.flink.connectors.bigquery.sink;

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import org.threeten.bp.Duration;

public class BigQueryStreamingAtLeastOnceSinkWriter extends BigQueryWriter {

  protected BigQueryStreamingAtLeastOnceSinkWriter(@Nonnull BigQueryConnectionOptions options) {
    super(options);
  }

  @Override
  protected StreamWriter getStreamWriter(
      BigQueryConnectionOptions options, BigQueryWriteClient client)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {

    StreamWriter.Builder streamWriterBuilder =
        StreamWriter.newBuilder(options.getTableName().toString() + "/_default", client);

    ProtoSchema protoSchema =
        ProtoSchemaConverter.convert(
            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                options.getTableSchema()));

    streamWriterBuilder
        .setWriterSchema(protoSchema)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
        .setChannelProvider(
            BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                .setKeepAliveTime(Duration.ofMinutes(1))
                .setKeepAliveTimeout(Duration.ofMinutes(1))
                .setKeepAliveWithoutCalls(true)
                .build());

    return streamWriterBuilder.build();
  }

  @Override
  protected void append(ProtoRows rows)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    append(new AppendContext(rows));
  }
}
