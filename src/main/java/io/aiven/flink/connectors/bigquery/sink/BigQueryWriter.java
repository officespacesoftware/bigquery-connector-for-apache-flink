package io.aiven.flink.connectors.bigquery.sink;

import static io.grpc.Status.Code.ALREADY_EXISTS;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Abstract class of BigQuery output format. */
public abstract class BigQueryWriter implements SinkWriter<ByteString> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriter.class);
  private static final ImmutableList<Status.Code> RETRIABLE_ERROR_CODES =
      ImmutableList.of(
          Status.Code.INTERNAL,
          Status.Code.ABORTED,
          Status.Code.CANCELLED,
          Status.Code.FAILED_PRECONDITION,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.UNAVAILABLE);
  protected static final int MAX_RECREATE_COUNT = 3;

  protected final BigQueryConnectionOptions options;
  protected transient BigQueryWriteClient client;

  protected transient StreamWriter streamWriter;

  // Track the number of in-flight requests to wait for all responses before
  // shutting down.
  protected transient Phaser inflightRequestCount;
  protected final AtomicInteger recreateCount = new AtomicInteger(0);

  protected final Serializable lock = new Serializable() {};

  @GuardedBy("lock")
  protected RuntimeException error = null;

  public BigQueryWriter(@Nonnull BigQueryConnectionOptions options) {
    this.options = Preconditions.checkNotNull(options);
    FixedCredentialsProvider creds = FixedCredentialsProvider.create(options.getCredentials());
    inflightRequestCount = new Phaser(1);
    BigQueryWriteSettings.Builder bigQueryWriteSettingsBuilder = BigQueryWriteSettings.newBuilder();
    bigQueryWriteSettingsBuilder
        .createWriteStreamSettings()
        .setRetrySettings(
            bigQueryWriteSettingsBuilder.createWriteStreamSettings().getRetrySettings().toBuilder()
                .setTotalTimeout(Duration.ofSeconds(30))
                .build());
    try {
      BigQueryWriteSettings bigQueryWriteSettings =
          bigQueryWriteSettingsBuilder.setCredentialsProvider(creds).build();
      client = BigQueryWriteClient.create(bigQueryWriteSettings);
      streamWriter = getStreamWriter(options, client);
    } catch (IOException | Descriptors.DescriptorValidationException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException, InterruptedException {}

  @Override
  public void close() throws IOException {
    // An error could happen before init of inflightRequestCount
    if (inflightRequestCount != null) {
      // Wait for all in-flight requests to complete.
      inflightRequestCount.arriveAndAwaitAdvance();
    }

    // streamWriter could fail to init
    if (streamWriter != null) {
      // Close the connection to the server.
      streamWriter.close();
    }

    // Verify that no error occurred in the stream.
    synchronized (this.lock) {
      if (this.error != null) {
        throw new IOException(this.error);
      }
    }

    if (client != null) {
      try {
        if (streamWriter != null) {
          client.finalizeWriteStream(streamWriter.getStreamName());
        }
      } finally {
        client.close();
      }
    }
  }

  // ** `context` is never used in this function */
  @Override
  public void write(ByteString bytes, Context context) throws IOException {
    ProtoRows protoRows = ProtoRows.newBuilder().addSerializedRows(bytes).build();
    try {
      append(protoRows);
    } catch (BigQueryException
        | Descriptors.DescriptorValidationException
        | InterruptedException
        | ExecutionException e) {
      throw new IOException(e);
    } catch (Exceptions.AppendSerializationError ase) {
      Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
      if (rowIndexToErrorMessage != null && !rowIndexToErrorMessage.isEmpty()) {
        throw new BigQueryConnectorRuntimeException(rowIndexToErrorMessage.toString(), ase);
      }
      throw ase;
    }
  }

  protected abstract StreamWriter getStreamWriter(
      BigQueryConnectionOptions options, BigQueryWriteClient client)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException;

  protected abstract void append(ProtoRows rows)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException,
          ExecutionException;

  public static class Builder {

    private BigQueryConnectionOptions options;

    public Builder() {}

    public Builder withOptions(BigQueryConnectionOptions options) {
      this.options = options;
      return this;
    }

    public BigQueryWriter build() {
      Preconditions.checkNotNull(options);
      if (options.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
        return new BigQueryStreamingAtLeastOnceSinkWriter(options);
      }

      throw new IllegalArgumentException(
          "Delivery guarantee " + options.getDeliveryGuarantee() + " is not supported");
    }
  }

  protected void append(AppendContext appendContext)
      throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
    synchronized (this.lock) {
      if (!streamWriter.isUserClosed()
          && streamWriter.isClosed()
          && recreateCount.getAndIncrement() < options.getRecreateCount()) {
        streamWriter =
            StreamWriter.newBuilder(streamWriter.getStreamName(), BigQueryWriteClient.create())
                .setWriterSchema(
                    ProtoSchemaConverter.convert(
                        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            options.getTableSchema())))
                .setExecutorProvider(
                    FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
                .setChannelProvider(
                    BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                        .setKeepAliveTime(Duration.ofMinutes(1))
                        .setKeepAliveTimeout(Duration.ofMinutes(1))
                        .setKeepAliveWithoutCalls(true)
                        .build())
                .build();
        this.error = null;
      }
      // If earlier appends have failed, we need to reset before continuing.
      if (this.error != null) {
        throw new IOException(this.error);
      }
    }
    // Append asynchronously for increased throughput.
    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
    ApiFutures.addCallback(
        future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

    // Increase the count of in-flight requests.
    inflightRequestCount.register();
  }

  protected static class AppendContext {

    private final ProtoRows data;
    private int retryCount = 0;

    AppendContext(ProtoRows data) {
      this.data = data;
    }
  }

  static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final BigQueryWriter parent;
    private final AppendContext appendContext;

    public AppendCompleteCallback(BigQueryWriter parent, AppendContext appendContext) {
      this.parent = parent;
      this.appendContext = appendContext;
    }

    @Override
    public void onSuccess(AppendRowsResponse response) {
      this.parent.recreateCount.set(0);
      done();
    }

    @Override
    public void onFailure(Throwable throwable) {
      // If the wrapped exception is a StatusRuntimeException, check the state of the
      // operation.
      // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more
      // information,
      // see:
      // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
      Status status = Status.fromThrowable(throwable);
      LOG.error("Received error for append: ", throwable);
      throwable.fillInStackTrace().printStackTrace();
      if (appendContext.retryCount < this.parent.options.getRetryCount()
          && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
        appendContext.retryCount++;
        LOG.error("Retrying append: " + appendContext.retryCount);
        try {
          // Since default stream appends are not ordered, we can simply retry the
          // appends.
          // Retrying with exclusive streams requires more careful consideration.
          this.parent.append(appendContext);
          // Mark the existing attempt as done since it's being retried.
          done();
          return;
        } catch (Exception e) {
          // Fall through to return error.
          LOG.error("Failed to retry append: ", e);
          // we should throw exception only when all attempts are used
          e.addSuppressed(throwable);
          throwable = e;
        }
      }
      if (status.getCode() == ALREADY_EXISTS) {
        LOG.info("Message for this offset already exists");
        done();
        return;
      }

      if (throwable instanceof Exceptions.AppendSerializationError) {
        Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) throwable;
        Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
        LOG.error("Append serialization error: " + rowIndexToErrorMessage.toString());
        if (!rowIndexToErrorMessage.isEmpty()) {
          // Omit the faulty rows
          ProtoRows.Builder dataBuilder = ProtoRows.newBuilder();
          for (int i = 0; i < appendContext.data.getSerializedRowsCount(); i++) {
            if (!rowIndexToErrorMessage.containsKey(i)) {
              dataBuilder.addSerializedRows(appendContext.data.getSerializedRows(i));
            } else {
              // process faulty rows by placing them on a dead-letter-queue, for instance
            }
          }
          ProtoRows dataNew = dataBuilder.build();

          // Retry the remaining valid rows, but using a separate thread to
          // avoid potentially blocking while we are in a callback.
          if (dataNew.getSerializedRowsCount() > 0) {
            try {
              this.parent.append(new AppendContext(dataNew));
            } catch (Descriptors.DescriptorValidationException
                | IOException
                | InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          // Mark the existing attempt as done since we got a response for it
          done();
          return;
        }
      }

      synchronized (this.parent.lock) {
        if (this.parent.error == null) {
          Exceptions.StorageException storageException = Exceptions.toStorageException(throwable);
          this.parent.error =
              (storageException != null) ? storageException : new RuntimeException(throwable);
        }
      }
      done();
    }

    private void done() {
      // Reduce the count of in-flight requests.
      this.parent.inflightRequestCount.arriveAndDeregister();
    }
  }
}
