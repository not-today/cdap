/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractSatisfiableCompositeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset that stores and indexes program schedules, so that they can be looked by their trigger keys.
 *
 * This uses an IndexedTable to allow reverse lookup. The table stores:
 * <ul>
 *   <li>Schedules: the row key is
 *     <code>&lt;namespace>.&lt;app-name>.&lt;app-version>.&lt;schedule-name>)</code>,
 *     which is globally unique (see {@link #rowKeyForSchedule(ScheduleId)}. The schedule itself is stored as JSON
 *     in the <code>sch</code> ({@link #SCHEDULE_COLUMN} column.</li>
 *   <li>Triggers: as every schedule can have multiple triggers, each trigger is stored and indexed in its row. The
 *     triggers of a schedule are enumerated, and each trigger is stored with a row key that is the same as the
 *     schedule's row key, with <code>@&lt;sequential-id></code> appended. This ensures that a schedules and its
 *     triggered are stored in adjacent rows. The only column of trigger row is the trigger key (that is, the
 *     key that can be constructed from an event to look up the schedules that have a trigger for it), in column
 *     <code>tk</code> ({@link #TRIGGER_KEY_COLUMN}</li>.
 * </ul>
 *
 * Lookup of schedules by trigger key is by first finding the all triggers for that event key (using the index),
 * then mapping each of these triggers to the schedule it belongs to.
 */
public class ProgramScheduleStoreDataset extends AbstractDataset {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramScheduleStoreDataset.class);

  private static final String SCHEDULE_COLUMN = "sch";
  private static final String UPDATED_COLUMN = "upd";
  private static final String STATUS_COLUMN = "sts";
  private static final String TRIGGER_KEY_COLUMN = "tk"; // trigger key
  private static final char TRIGGER_SEPARATOR = '@';
  private static final String ROW_KEY_SEPARATOR = ":";

  private static final byte[] SCHEDULE_COLUMN_BYTES = Bytes.toBytes(SCHEDULE_COLUMN);
  private static final byte[] UPDATED_COLUMN_BYTES = Bytes.toBytes(UPDATED_COLUMN);
  private static final byte[] STATUS_COLUMN_BYTES = Bytes.toBytes(STATUS_COLUMN);
  private static final byte[] TRIGGER_KEY_COLUMN_BYTES = Bytes.toBytes(TRIGGER_KEY_COLUMN);
  private static final byte[] TRIGGER_SEPARATOR_BYTES = Bytes.toBytes("" + TRIGGER_SEPARATOR);

  // package visible for the dataset definition
  static final String EMBEDDED_TABLE_NAME = "it"; // indexed table
  static final String INDEX_COLUMNS = TRIGGER_KEY_COLUMN; // trigger key

  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .create();

  private final IndexedTable store;

  ProgramScheduleStoreDataset(DatasetSpecification spec,
                              @EmbeddedDataset(EMBEDDED_TABLE_NAME) IndexedTable store) {
    super(spec.getName(), store);
    this.store = store;
  }

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @return the new schedule's last modified timestamp
   * @throws AlreadyExistsException if the schedule already exists
   */
  public long addSchedule(ProgramSchedule schedule) throws AlreadyExistsException {
    return addSchedules(Collections.singleton(schedule));
  }

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @param status the status of the schedule to add
   * @param currentTime the current time in milliseconds when adding the schedule
   * @return the new schedule's last modified timestamp
   * @throws AlreadyExistsException if the schedule already exists
   */
  private void addScheduleWithStatus(ProgramSchedule schedule, ProgramScheduleStatus status, long currentTime)
    throws AlreadyExistsException {
    byte[] scheduleKey = rowKeyBytesForSchedule(schedule.getProgramId().getParent().schedule(schedule.getName()));
    if (!store.get(new Get(scheduleKey)).isEmpty()) {
      throw new AlreadyExistsException(schedule.getProgramId().getParent().schedule(schedule.getName()));
    }
    Put schedulePut = new Put(scheduleKey);
    schedulePut.add(SCHEDULE_COLUMN_BYTES, GSON.toJson(schedule));
    schedulePut.add(UPDATED_COLUMN_BYTES, currentTime);
    schedulePut.add(STATUS_COLUMN_BYTES, status.toString());
    store.put(schedulePut);
    int count = 0;
    for (String triggerKey : extractTriggerKeys(schedule)) {
      byte[] triggerRowKey = rowKeyBytesForTrigger(scheduleKey, count++);
      store.put(new Put(triggerRowKey, TRIGGER_KEY_COLUMN_BYTES, triggerKey));
    }
  }

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   * @return the new schedules' last modified timestamp
   * @throws AlreadyExistsException if one of the schedules already exists
   */
  public long addSchedules(Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException {
    long currentTime = System.currentTimeMillis();
    for (ProgramSchedule schedule : schedules) {
      addScheduleWithStatus(schedule, ProgramScheduleStatus.SUSPENDED, currentTime); // initially suspended
    }
    return currentTime;
  }

  /**
   * Update the status of a schedule. This also updates the last-updated timestamp.
   * @return the updated schedule's last modified timestamp
   */
  public long updateScheduleStatus(ScheduleId scheduleId, ProgramScheduleStatus newStatus) throws NotFoundException {
    long currentTime = System.currentTimeMillis();
    String scheduleKey = rowKeyForSchedule(scheduleId);
    Row row = store.get(new Get(scheduleKey));
    if (row.isEmpty()) {
      throw new NotFoundException(scheduleId);
    }
    Put updatePut = new Put(scheduleKey);
    updatePut.add(UPDATED_COLUMN_BYTES, currentTime); // record current time
    updatePut.add(STATUS_COLUMN_BYTES, newStatus.toString());
    store.put(updatePut);
    return currentTime;
  }

  /**
   * Update an existing schedule in the store.
   *
   * @param schedule the schedule to update
   * @return the updated schedule's last modified timestamp
   * @throws NotFoundException if one of the schedules already exists
   */
  public long updateSchedule(ProgramSchedule schedule) throws NotFoundException {
    deleteSchedule(schedule.getScheduleId());
    try {
      return addSchedule(schedule);
    } catch (AlreadyExistsException e) {
      // Should never reach here because we just deleted it
      throw new IllegalStateException(
        "Schedule '" + schedule.getScheduleId() + "' already exists despite just being deleted.");
    }
  }

  /**
   * Removes a schedule from the store. Succeeds whether the schedule exists or not.
   *
   * @param scheduleId the schedule to delete
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public void deleteSchedule(ScheduleId scheduleId) throws NotFoundException {
    deleteSchedules(Collections.singleton(scheduleId));
  }

  /**
   * Removes one or more schedules from the store. Succeeds whether the schedules exist or not.
   *
   * @param scheduleIds the schedules to delete
   * @throws NotFoundException if one of the schedules does not exist in the store
   */
  public void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {
    for (ScheduleId scheduleId : scheduleIds) {
      String scheduleKey = rowKeyForSchedule(scheduleId);
      if (store.get(new Get(scheduleKey)).isEmpty()) {
        throw new NotFoundException(scheduleId);
      }
      store.delete(new Delete(scheduleKey));
      byte[] prefix = keyPrefixForTriggerScan(scheduleKey);
      try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
        Row row;
        while ((row = scanner.next()) != null) {
          store.delete(row.getRow());
        }
      }
    }
  }

  /**
   * Removes all schedules for a specific application from the store.
   *
   * @param appId the application id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  public List<ScheduleId> deleteSchedules(ApplicationId appId) {
    List<ScheduleId> deleted = new ArrayList<>();
    // since all trigger row keys are prefixed by <scheduleRowKey>@,
    // a scan for that prefix finds exactly the schedules and all of its triggers
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        store.delete(row.getRow());
        deleted.add(rowKeyToScheduleId(row.getRow()));
      }
    }
    return deleted;
  }

  /**
   * Removes all schedules for a specific program from the store.
   *
   * @param programId the program id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  public List<ScheduleId> deleteSchedules(ProgramId programId) {
    List<ScheduleId> deleted = new ArrayList<>();
    // since all trigger row keys are prefixed by <scheduleRowKey>@,
    // a scan for that prefix finds exactly the schedules and all of its triggers
    byte[] prefix = keyPrefixForApplicationScan(programId.getParent());
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          if (programId.equals(schedule.getProgramId())) {
            store.delete(row.getRow());
            deleted.add(schedule.getScheduleId());
          }
        }
      }
    }
    return deleted;
  }

  /**
   * Update all schedules that can be triggered by the given deleted program. A schedule will be removed if
   * the only {@link ProgramStatusTrigger} in it is triggered by the deleted program. Schedules with composite triggers
   * will be updated if the composite trigger can still be satisfied after the program is deleted, otherwise the
   * schedules will be deleted.
   *
   * @param programId the program id for which to delete the schedules
   * @return the IDs of the schedules that were deleted
   */
  public List<ScheduleId> modifySchedulesTriggeredByDeletedProgram(ProgramId programId) {
    List<ScheduleId> deleted = new ArrayList<>();
    Set<ProgramScheduleRecord> scheduleRecords = new HashSet<>();
    for (ProgramStatus status : ProgramStatus.values()) {
      scheduleRecords.addAll(findSchedules(Schedulers.triggerKeyForProgramStatus(programId, status)));
    }
    for (ProgramScheduleRecord scheduleRecord : scheduleRecords) {
      ProgramSchedule schedule = scheduleRecord.getSchedule();
      try {
        deleteSchedule(schedule.getScheduleId());
      } catch (NotFoundException e) {
        // this should never happen
        LOG.warn("Failed to delete the schedule '{}' triggered by '{}', skip this schedule.",
                 schedule.getScheduleId(), programId, e);
        continue;
      }
      if (schedule.getTrigger() instanceof AbstractSatisfiableCompositeTrigger) {
        // get the updated composite trigger by removing the program status trigger of the given program
        Trigger updatedTrigger =
          ((AbstractSatisfiableCompositeTrigger) schedule.getTrigger()).getTriggerWithDeletedProgram(programId);
        if (updatedTrigger == null) {
          deleted.add(schedule.getScheduleId());
          continue;
        }
        // if the updated composite trigger is not null, add the schedule back with updated composite trigger
        try {
          addScheduleWithStatus(new ProgramSchedule(schedule.getName(), schedule.getDescription(),
                                                    schedule.getProgramId(), schedule.getProperties(), updatedTrigger,
                                                    schedule.getConstraints(), schedule.getTimeoutMillis()),
                                scheduleRecord.getMeta().getStatus(), System.currentTimeMillis());
        } catch (AlreadyExistsException e) {
          // this should never happen
          LOG.warn("Failed to add the schedule '{}' triggered by '{}' with updated trigger '{}', " +
                     "skip adding this schedule.", schedule.getScheduleId(), programId, updatedTrigger, e);
        }
      } else {
        deleted.add(schedule.getScheduleId());
      }
    }
    return deleted;
  }

  /**
   * Read a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException {
    Row row = store.get(new Get(rowKeyForSchedule(scheduleId)));
    byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
    if (serialized == null) {
      throw new NotFoundException(scheduleId);
    }
    return GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
  }

  /**
   * Read the meta data for a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the stored meta data for the schedule, or null if the schedule does not exist
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public @Nullable ProgramScheduleMeta getScheduleMeta(ScheduleId scheduleId) throws NotFoundException {
    Row row = store.get(new Get(rowKeyBytesForSchedule(scheduleId), UPDATED_COLUMN_BYTES, STATUS_COLUMN_BYTES));
    if (row.isEmpty()) {
      return null;
    }
    return extractMetaFromRow(scheduleId, row);
  }

  /**
   * Read all information about a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule record from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  public ProgramScheduleRecord getScheduleRecord(ScheduleId scheduleId) throws NotFoundException {
    Row row = store.get(new Get(rowKeyForSchedule(scheduleId)));
    byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
    if (serialized == null) {
      throw new NotFoundException(scheduleId);
    }
    ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
    ProgramScheduleMeta meta = extractMetaFromRow(scheduleId, row);
    return new ProgramScheduleRecord(schedule, meta);
  }

  /**
   * Retrieve all schedules for a given application.
   *
   * @param appId the application for which to list the schedules.
   * @return a list of schedules for the application; never null
   */
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return listSchedules(appId, null);
  }

  /**
   * Retrieve all schedules for a given program.
   *
   * @param programId the program for which to list the schedules.
   * @return a list of schedules for the program; never null
   */
  public List<ProgramSchedule> listSchedules(ProgramId programId) {
    return listSchedules(programId.getParent(), programId);
  }

  /**
   * Retrieve all schedule records for a given application.
   *
   * @param appId the application for which to list the schedule records.
   * @return a list of schedule records for the application; never null
   */
  public List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId) {
    return listScheduleRecords(appId, null);
  }

  /**
   * Retrieve all schedule records for a given program.
   *
   * @param programId the program for which to list the schedule records.
   * @return a list of schedule records for the program; never null
   */
  public List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId) {
    return listScheduleRecords(programId.getParent(), programId);
  }

  /**
   * Find all schedules that have a trigger with a given trigger key.
   *
   * @param triggerKey the trigger key to look up
   * @return a list of all schedules that are triggered by this key; never null
   */
  public Collection<ProgramScheduleRecord> findSchedules(String triggerKey) {
    Map<ScheduleId, ProgramScheduleRecord> schedulesFound = new HashMap<>();
    try (Scanner scanner = store.readByIndex(TRIGGER_KEY_COLUMN_BYTES, Bytes.toBytes(triggerKey))) {
      Row triggerRow;
      while ((triggerRow = scanner.next()) != null) {
        String triggerRowKey = Bytes.toString(triggerRow.getRow());
        try {
          ScheduleId scheduleId = extractScheduleIdFromTriggerKey(triggerRowKey);
          if (schedulesFound.containsKey(scheduleId)) {
            continue;
          }
          Row row = store.get(new Get(rowKeyForSchedule(scheduleId)));
          byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
          if (serialized == null) {
            throw new NotFoundException(scheduleId);
          }
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          ProgramScheduleMeta meta = extractMetaFromRow(scheduleId, row);
          ProgramScheduleRecord record = new ProgramScheduleRecord(schedule, meta);
          schedulesFound.put(scheduleId, record);
        } catch (IllegalArgumentException | NotFoundException e) {
          // the only exceptions we know to be thrown here are IllegalArgumentException (ill-formed key) or
          // NotFoundException (if the schedule does not exist). Both should never happen, so we warn and ignore.
          // we will let any other exception propagate up, because it would be a DataSetException or similarly serious.
          LOG.warn("Problem with trigger id '{}' found for trigger key '{}': {}. Skipping entry.",
                   triggerRowKey, triggerKey, e.getMessage());
        }
      }
    }
    return schedulesFound.values();
  }

  /*------------------- private helpers ---------------------*/

  /**
   * List schedules in a given application and if the programId is not null, only return the schedules
   * which can launch the given program
   */
  private List<ProgramSchedule> listSchedules(ApplicationId appId, @Nullable ProgramId programId) {
    List<ProgramSchedule> result = new ArrayList<>();
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          if (programId == null || programId.equals(schedule.getProgramId())) {
            result.add(schedule);
          }
        }
      }
    }
    return result;
  }

  /**
   * List schedule records in a given application and if the programId is not null, only return the schedules
   * which can launch the given program
   */
  private List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId, @Nullable ProgramId programId) {
    List<ProgramScheduleRecord> result = new ArrayList<>();
    byte[] prefix = keyPrefixForApplicationScan(appId);
    try (Scanner scanner = store.scan(new Scan(prefix, Bytes.stopKeyForPrefix(prefix)))) {
      Row row;
      while ((row = scanner.next()) != null) {
        byte[] serialized = row.get(SCHEDULE_COLUMN_BYTES);
        if (serialized != null) {
          ProgramSchedule schedule = GSON.fromJson(Bytes.toString(serialized), ProgramSchedule.class);
          if (programId == null || programId.equals(schedule.getProgramId())) {
            result.add(new ProgramScheduleRecord(schedule, extractMetaFromRow(schedule.getScheduleId(), row)));
          }
        }
      }
    }
    return result;
  }

  /**
   * Reads the meta data from a row in the schedule store.
   *
   * @throws IllegalStateException if one of the expected fields is missing or ill-formed.
   */
  private ProgramScheduleMeta extractMetaFromRow(ScheduleId scheduleId, Row row) {
    Long updatedTime = row.getLong(UPDATED_COLUMN_BYTES);
    String statusString = row.getString(STATUS_COLUMN_BYTES);
    try {
      Preconditions.checkArgument(updatedTime != null, "Last-updated timestamp is null");
      Preconditions.checkArgument(statusString != null, "schedule status is null");
      ProgramScheduleStatus status = ProgramScheduleStatus.valueOf(statusString);
      return new ProgramScheduleMeta(status, updatedTime);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
        String.format("Unexpected stored meta data for schedule %s: %s", scheduleId, e.getMessage()));
    }
  }

  /**
   * This method extracts all trigger keys from a schedule. These are the keys for which we need to index
   * the schedule, so that we can do a reverse lookup for an event received.
   * <p>
   * For now, we do not support composite trigger, but in the future this is where the triggers need to be
   * extracted from composite triggers. Hence the return type of this method is a list.
   */
  private static Set<String> extractTriggerKeys(ProgramSchedule schedule) {
    return ((SatisfiableTrigger) schedule.getTrigger()).getTriggerKeys();
  }

  private static String rowKeyForSchedule(ScheduleId scheduleId) {
    return Joiner.on(ROW_KEY_SEPARATOR).join(scheduleId.toIdParts());
  }

  private static byte[] rowKeyBytesForSchedule(ScheduleId scheduleId) {
    return Bytes.toBytes(rowKeyForSchedule(scheduleId));
  }

  private static ScheduleId rowKeyToScheduleId(byte[] rowKey) {
    return rowKeyToScheduleId(Bytes.toString(rowKey));
  }

  private static ScheduleId rowKeyToScheduleId(String rowKey) {
    return ScheduleId.fromIdParts(Lists.newArrayList(rowKey.split(ROW_KEY_SEPARATOR)));
  }

  private static byte[] rowKeyBytesForTrigger(byte[] scheduleRowKey, int count) {
    return Bytes.add(scheduleRowKey, TRIGGER_SEPARATOR_BYTES, Bytes.toBytes(Integer.toString(count)));
  }

  private static ScheduleId extractScheduleIdFromTriggerKey(String triggerRowKey) {
    int index = triggerRowKey.lastIndexOf(TRIGGER_SEPARATOR);
    if (index > 0) {
      return rowKeyToScheduleId(triggerRowKey.substring(0, index));
    }
    throw new IllegalArgumentException(
      "Trigger key is expected to be of the form <scheduleId>@<n> but is '" + triggerRowKey + "' (no '@' found)");
  }

  private static byte[] keyPrefixForTriggerScan(String scheduleRowKey) {
    return Bytes.toBytes(scheduleRowKey + '@');
  }

  private static byte[] keyPrefixForApplicationScan(ApplicationId appId) {
    return Bytes.toBytes(
      Joiner.on(ROW_KEY_SEPARATOR).join(appId.getNamespace(), appId.getApplication(), appId.getVersion(), ""));
  }
}
