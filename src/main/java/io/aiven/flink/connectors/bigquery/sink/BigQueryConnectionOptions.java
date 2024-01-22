package io.aiven.flink.connectors.bigquery.sink;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.io.Serializable;

public class BigQueryConnectionOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Credentials credentials;

  private final String project;
  private final String dataset;
  private final String table;
  private transient TableSchema tableSchema;
  private final byte[] tableSchemeBytes;
  private final long maxOutstandingElementsCount;
  private final long maxOutstandingRequestBytes;
  private final int retryCount;
  private final int recreateCount;

  private final boolean createIfNotExists;
  private final DeliveryGuarantee deliveryGuarantee;

  public BigQueryConnectionOptions(
      String project,
      String dataset,
      String table,
      TableSchema tableSchema,
      boolean createIfNotExists,
      DeliveryGuarantee deliveryGuarantee,
      long maxOutstandingElementsCount,
      long maxOutstandingRequestBytes,
      int retryCount,
      int recreateCount,
      Credentials credentials) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.tableSchema = tableSchema;
    this.tableSchemeBytes = tableSchema.toByteArray();
    this.createIfNotExists = createIfNotExists;
    this.deliveryGuarantee = deliveryGuarantee;
    this.credentials = credentials;
    this.maxOutstandingElementsCount = maxOutstandingElementsCount;
    this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
    this.retryCount = retryCount;
    this.recreateCount = recreateCount;
  }

  public TableName getTableName() {
    return TableName.of(project, dataset, table);
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public boolean isCreateIfNotExists() {
    return createIfNotExists;
  }

  public DeliveryGuarantee getDeliveryGuarantee() {
    return deliveryGuarantee;
  }

  public long getMaxOutstandingElementsCount() {
    return maxOutstandingElementsCount;
  }

  public long getMaxOutstandingRequestBytes() {
    return maxOutstandingRequestBytes;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public int getRecreateCount() {
    return recreateCount;
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.tableSchema = TableSchema.parseFrom(tableSchemeBytes);
  }
}
