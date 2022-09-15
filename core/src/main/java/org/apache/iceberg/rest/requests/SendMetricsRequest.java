/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.requests;

import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class SendMetricsRequest implements RESTRequest {

  public enum MetricsType {
    UNKNOWN,
    SCAN_REPORT
  }

  private ScanReport scanReport;
  private MetricsType metricsType;

  @SuppressWarnings("unused")
  public SendMetricsRequest() {
    // Needed for Jackson Deserialization.
  }

  private SendMetricsRequest(ScanReport scanReport) {
    this.scanReport = scanReport;
    metricsType = null != scanReport ? MetricsType.SCAN_REPORT : MetricsType.UNKNOWN;
    validate();
  }

  public ScanReport scanReport() {
    return scanReport;
  }

  public MetricsType getMetricsType() {
    return metricsType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("scanReport", scanReport).toString();
  }

  @Override
  public void validate() {
    // new metric types might be added here, so we need to make sure that exactly one is not-null
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");
  }

  public static SendMetricsRequest.Builder builder() {
    return new SendMetricsRequest.Builder();
  }

  public static class Builder {
    private ScanReport scanReport;

    private Builder() {}

    public Builder fromScanReport(ScanReport newScanReport) {
      this.scanReport = newScanReport;
      return this;
    }

    public SendMetricsRequest build() {
      // new metric types might be added here, so we need to make sure that exactly one is not-null
      Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");
      return new SendMetricsRequest(scanReport);
    }
  }
}
