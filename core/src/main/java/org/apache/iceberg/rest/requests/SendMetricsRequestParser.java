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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.metrics.ScanReportParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.SendMetricsRequest.MetricsType;
import org.apache.iceberg.util.JsonUtil;

public class SendMetricsRequestParser {

  private static final String METRICS_TYPE = "metrics-type";
  private static final String METRICS_VALUE = "metrics-value";

  private SendMetricsRequestParser() {}

  public static String toJson(SendMetricsRequest request) {
    return toJson(request, false);
  }

  public static String toJson(SendMetricsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(SendMetricsRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid metrics request: null");

    gen.writeStartObject();
    gen.writeStringField(METRICS_TYPE, request.getMetricsType().name().toLowerCase(Locale.ROOT));
    gen.writeFieldName(METRICS_VALUE);
    metricsToJson(request, gen);
    gen.writeEndObject();
  }

  private static void metricsToJson(SendMetricsRequest request, JsonGenerator gen)
      throws IOException {
    if (null != request.scanReport()) {
      ScanReportParser.toJson(request.scanReport(), gen);
    }
  }

  public static SendMetricsRequest fromJson(String json) {
    return JsonUtil.parse(json, SendMetricsRequestParser::fromJson);
  }

  public static SendMetricsRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse metrics request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse metrics request from non-object: %s", json);

    String type = JsonUtil.getString(METRICS_TYPE, json);
    if (MetricsType.SCAN_REPORT == MetricsType.valueOf(type.toUpperCase(Locale.ROOT))) {
      return SendMetricsRequest.builder()
          .fromScanReport(ScanReportParser.fromJson(JsonUtil.get(METRICS_VALUE, json)))
          .build();
    }
    throw new IllegalArgumentException(String.format("Cannot build metrics request from %s", json));
  }
}
