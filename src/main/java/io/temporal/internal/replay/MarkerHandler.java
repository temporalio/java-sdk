/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.internal.replay;

import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.PayloadConverter;
import io.temporal.internal.sync.WorkflowInternal;
import io.temporal.proto.common.Header;
import io.temporal.proto.common.Payload;
import io.temporal.proto.common.Payloads;
import io.temporal.proto.event.EventType;
import io.temporal.proto.event.HistoryEvent;
import io.temporal.proto.event.MarkerRecordedEventAttributes;
import io.temporal.workflow.Functions.Func1;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class MarkerHandler {
  // Including mutable side effect and version marker.
  private static final String MUTABLE_MARKER_HEADER_KEY = "MutableMarkerHeader";

  private static final class MarkerResult {

    private final Optional<Payloads> data;

    /**
     * Count of how many times handle was called since the last marker recorded. It is used to
     * ensure that an updated value is returned after the same exact number of times during a
     * replay.
     */
    private int accessCount;

    private MarkerResult(Optional<Payloads> data) {
      this.data = data;
    }

    public Optional<Payloads> getData() {
      accessCount++;
      return data;
    }

    int getAccessCount() {
      return accessCount;
    }
  }

  interface MarkerInterface {
    String getId();

    long getEventId();

    int getAccessCount();

    Optional<Payloads> getData();

    static MarkerInterface fromEventAttributes(
        MarkerRecordedEventAttributes attributes, DataConverter converter) {
      Optional<Payloads> details =
          attributes.hasDetails() ? Optional.of(attributes.getDetails()) : Optional.empty();
      if (attributes.hasHeader()) {
        Header markerHeader = attributes.getHeader();
        if (markerHeader.containsFields(MUTABLE_MARKER_HEADER_KEY)) {
          MarkerData.MarkerHeader header =
              converter
                  .getPayloadConverter()
                  .fromData(
                      markerHeader.getFieldsOrThrow(MUTABLE_MARKER_HEADER_KEY),
                      MarkerData.MarkerHeader.class,
                      MarkerData.MarkerHeader.class);

          return new MarkerData(header, details);
        }
      }
      return converter.fromData(details, PlainMarkerData.class, PlainMarkerData.class);
    }
  }

  static final class MarkerData implements MarkerInterface {

    private static final class MarkerHeader {
      private final String id;
      private final long eventId;
      private final int accessCount;

      MarkerHeader(String id, long eventId, int accessCount) {
        this.id = id;
        this.eventId = eventId;
        this.accessCount = accessCount;
      }
    }

    private final MarkerHeader header;
    private final Optional<Payloads> data;

    MarkerData(String id, long eventId, Optional<Payloads> data, int accessCount) {
      this.header = new MarkerHeader(id, eventId, accessCount);
      this.data = data;
    }

    MarkerData(MarkerHeader header, Optional<Payloads> data) {
      this.header = header;
      this.data = data;
    }

    @Override
    public String getId() {
      return header.id;
    }

    @Override
    public long getEventId() {
      return header.eventId;
    }

    @Override
    public Optional<Payloads> getData() {
      return data;
    }

    @Override
    public int getAccessCount() {
      return header.accessCount;
    }

    Header getHeader(PayloadConverter converter) {
      Optional<Payload> headerData = converter.toData(header);
      return Header.newBuilder().putFields(MUTABLE_MARKER_HEADER_KEY, headerData.get()).build();
    }
  }

  static final class PlainMarkerData implements MarkerInterface {

    private final String id;
    private final long eventId;
    private final Optional<Payloads> data;
    private final int accessCount;

    PlainMarkerData(String id, long eventId, Optional<Payloads> data, int accessCount) {
      this.id = id;
      this.eventId = eventId;
      this.data = data;
      this.accessCount = accessCount;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public long getEventId() {
      return eventId;
    }

    @Override
    public Optional<Payloads> getData() {
      return data;
    }

    @Override
    public int getAccessCount() {
      return accessCount;
    }
  }

  private final DecisionsHelper decisions;
  private final String markerName;
  private final ReplayAware replayContext;

  // Key is marker id
  private final Map<String, MarkerResult> mutableMarkerResults = new HashMap<>();

  MarkerHandler(DecisionsHelper decisions, String markerName, ReplayAware replayContext) {
    this.decisions = decisions;
    this.markerName = markerName;
    this.replayContext = replayContext;
  }

  /**
   * @param id marker id
   * @param func given the value from the last marker returns value to store. If result is empty
   *     nothing is recorded into the history.
   * @return the latest value returned by func
   */
  Optional<Payloads> handle(
      String id, DataConverter converter, Func1<Optional<Payloads>, Optional<Payloads>> func) {
    MarkerResult result = mutableMarkerResults.get(id);
    Optional<Payloads> stored;
    if (result == null) {
      stored = Optional.empty();
    } else {
      stored = result.getData();
    }
    long eventId = decisions.getNextDecisionEventId();
    int accessCount = result == null ? 0 : result.getAccessCount();

    if (replayContext.isReplaying()) {
      Optional<Payloads> data = getMarkerDataFromHistory(eventId, id, accessCount, converter);
      if (data.isPresent()) {
        // Need to insert marker to ensure that eventId is incremented
        recordMutableMarker(id, eventId, data, accessCount, converter);
        return data;
      }

      // TODO(maxim): Verify why this is necessary.
      if (!stored.isPresent()) {
        mutableMarkerResults.put(
            id, new MarkerResult(converter.toData(WorkflowInternal.DEFAULT_VERSION)));
      }

      return stored;
    }
    Optional<Payloads> toStore = func.apply(stored);
    if (toStore.isPresent()) {
      recordMutableMarker(id, eventId, toStore, accessCount, converter);
      return toStore;
    }
    return stored;
  }

  private Optional<Payloads> getMarkerDataFromHistory(
      long eventId, String markerId, int expectedAcccessCount, DataConverter converter) {
    Optional<HistoryEvent> event = decisions.getOptionalDecisionEvent(eventId);
    if (!event.isPresent() || event.get().getEventType() != EventType.MarkerRecorded) {
      return Optional.empty();
    }

    MarkerRecordedEventAttributes attributes = event.get().getMarkerRecordedEventAttributes();
    String name = attributes.getMarkerName();
    if (!markerName.equals(name)) {
      return Optional.empty();
    }

    MarkerInterface markerData = MarkerInterface.fromEventAttributes(attributes, converter);
    // access count is used to not return data from the marker before the recorded number of calls
    if (!markerId.equals(markerData.getId())
        || markerData.getAccessCount() > expectedAcccessCount) {
      return Optional.empty();
    }
    return markerData.getData();
  }

  private void recordMutableMarker(
      String id, long eventId, Optional<Payloads> data, int accessCount, DataConverter converter) {
    MarkerData marker = new MarkerData(id, eventId, data, accessCount);
    mutableMarkerResults.put(id, new MarkerResult(data));
    decisions.recordMarker(markerName, marker.getHeader(converter.getPayloadConverter()), data);
  }
}
