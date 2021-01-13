// Copyright 2018 The Ripple Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "binlog_position.h"

#include "logging.h"
#include "monitoring.h"
#include "mysql_constants.h"
#include "byte_order.h"

namespace mysql_ripple {

void parseTableMapEvent(const uint8_t *buffer, int len) {
  // https://dev.mysql.com/doc/internals/en/table-map-event.html
  uint64_t table_id;
  std::string schema_name;
  std::string table_name;

  table_id = byte_order::load6(buffer);

  // 1              schema name length
  // string         schema name
  // 1              [00]
  // 1              table name length
  // string         table name

  uint8_t schema_name_len = byte_order::load1(buffer + 8);
  schema_name.assign(reinterpret_cast<const char*>(buffer + 8 + 1), schema_name_len);

  uint8_t table_name_len = byte_order::load1(buffer + 8 + 1 + schema_name_len + 1);
  table_name.assign(reinterpret_cast<const char*>(buffer + 8 + 1 + schema_name_len + 2), table_name_len);

  if(schema_name.compare("mysql") != 0) {
    LOG(INFO) << "TABLE_MAP_EVENT; " << schema_name << "." << table_name
            << " table_id=" << std::to_string(table_id);
  }
}

void printHex(const uint8_t *buffer, int len) {
    for(size_t i = 0; i < len; ++i)
    fprintf(stdout, "0x%02X%s", buffer[i],
             ( i + 1 ) % 16 == 0 ? "\r\n" : " " );
}

// 00 00 01 00 00 00
// 6F 00
// 00 00 00 00 01 00 02 00 05 FF E0 1B 07 00
// 00 00 00 00 00 10 0E 00 00 01 04 57 45 53 54 E0
// 1B 07 00 00 01 00 00 00 00 00 00 00 00 03 57 45
// I0113 20:16:38.843719  6927 binlog_position.cc:68] ROWS_EVENT; extra_len=2
// I0113 20:16:38.843729  6927 binlog_position.cc:75] ROWS_EVENT; table_id=111 columns=224

// 00 00 00 00 00 6D
// 00 00 //flags
// 00 00 // extra-data-length
// 00 01 00 02 00 02 FF FE 0B 00 00
// I0113 20:16:38.844470  6927 binlog_position.cc:68] ROWS_EVENT; extra_len=2
// I0113 20:16:38.844476  6927 binlog_position.cc:75] ROWS_EVENT; table_id=109 columns=254

void parseRowsEvent(uint8_t type_code, const uint8_t *buffer, int len) {
  // TODO: branch on type_code

  uint64_t table_id = byte_order::load6(buffer);

  if(len < 120) {
    printHex(buffer, len);
  }

  // buffer + 6 = flags
  // buffer + 6 + 2 = extra-data-length

  uint16_t extra_len = byte_order::load2(buffer + 6 + 2);
  LOG(INFO) << "ROWS_EVENT; extra_len=" << std::to_string(extra_len) << " len byte1: " << std::to_string(buffer[6 + 2]) << " len byte2: " << std::to_string(buffer[6 + 2 + 1]);

  // lenenc number of columns
  // why 11?
  uint8_t col_num = byte_order::load1(buffer + 11);

  // skip (num of columns+7)/8

  LOG(INFO) << "ROWS_EVENT; " << "table_id=" << std::to_string(table_id) << " columns=" << std::to_string(col_num);
}

int BinlogPosition::Update(RawLogEventData event, off_t end_offset) {
  next_master_position.offset = event.header.nextpos;
  latest_master_position = next_master_position;
  latest_event_start_position = latest_event_end_position;
  latest_event_end_position.offset = end_offset;

  switch (event.header.type) {
    case constants::ET_FORMAT_DESCRIPTION: {
      FormatDescriptorEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse FormatDescriptorEvent";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_FD);
        return -1;
      }
      if (group_state != NO_GROUP) {
        LOG(ERROR) << "Incorrect group state when receiving FormatDescriptor"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }

      FormatDescriptorEvent *dst = nullptr;
      // Each file has first own format and then master format.
      if (own_format.IsEmpty()) {
        dst = &own_format;
      } else {
        dst = &master_format;
      }

      if (!(dst->IsEmpty() || dst->EqualExceptTimestamp(ev))) {
        LOG(ERROR) << "Failed to apply new format descriptor!"
                   << "\ncurrent: " << dst->ToInfoString()
                   << "\nnew: " << dst->ToInfoString();
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_APPLY_FD);
        abort();
        return -1;
      }
      *dst = ev;
      if (dst == &master_format) {
        master_server_id.assign(event.header.server_id);
      }
      break;
    }
    case constants::ET_ROTATE: {
      RotateEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse RotateEvent";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_EVENT);
        return -1;
      }
      if (group_state != NO_GROUP) {
        LOG(ERROR) << "Incorrect group state when receiving RotateEvent"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }
      next_master_position.filename = ev.filename;
      next_master_position.offset = ev.offset;
      break;
    }
    case constants::ET_GTID_MARIADB: {
      GTIDEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse GTIDEvent (MariaDB)";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_GTID);
        return -1;
      }
      if (group_state != NO_GROUP) {
        LOG(ERROR) << "Incorrect group state when receiving GTIDEvent"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }
      if (!gtid_start_position.ValidSuccessor(ev.gtid)) {
        LOG(ERROR) << "Received gtid: " << ev.gtid.ToString()
                   << " that is not valid successor to "
                   << gtid_start_position.ToString();
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_GTID_NOT_VALID);
        return -1;
      }
      if (ev.is_standalone)
        group_state = STANDALONE;
      else
        group_state = IN_TRANSACTION;
      latest_start_gtid = ev.gtid;
      break;
    }
    case constants::ET_GTID_MYSQL: {
      GTIDMySQLEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse GTIDEvent (MySQL)";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_GTID);
        return -1;
      }
      if (group_state != NO_GROUP) {
        LOG(ERROR) << "Incorrect group state when receiving GTIDEvent"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }
      if (!gtid_start_position.ValidSuccessor(ev.gtid)) {
        LOG(ERROR) << "Received gtid: " << ev.gtid.ToString()
                   << " that is not valid successor to "
                   << gtid_start_position.ToString();
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_GTID_NOT_VALID);
        return -1;
      }

      // MySQL does not mark the GTID-event as standalone/transactional
      // but instead puts the BEGIN event into the log.
      group_state = STANDALONE;
      latest_start_gtid = ev.gtid;
      break;
    }
    case constants::ET_XID: {
      XIDEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse XIDEvent";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_XID);
        return -1;
      }
      if (group_state != IN_TRANSACTION) {
        LOG(ERROR) << "Incorrect group state when receiving XIDEvent"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }
      group_state = END_OF_GROUP;
      break;
    }
    case constants::ET_TABLE_MAP: {
      parseTableMapEvent(event.event_data, event.event_data_length);
      break;
    }
    // TODO: need to handle UPDATE_ROWS and DELETE_ROWS as well
    case constants::ET_WRITE_ROWS_V2: {
      parseRowsEvent(constants::ET_WRITE_ROWS_V2, event.event_data, event.event_data_length);
      break;
    }
    case constants::ET_QUERY: {
      QueryEvent ev;
      if (!ev.ParseFromRawLogEventData(event)) {
        LOG(ERROR) << "Failed to parse QueryEvent";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_PARSE_QUERY);
        return -1;
      }

      if (ev.query.compare("BEGIN") == 0) {
        // MySQL does not mark the GTID-event as standalone/transactional
        // but instead puts the BEGIN event into the log.
        if (group_state == STANDALONE)
          group_state = IN_TRANSACTION;
        goto unparsed;
      }

      if (ev.query.compare("COMMIT") != 0 &&
          ev.query.compare("ROLLBACK") != 0) {
        // If query is not COMMIT/ROLLBACK
        // then treat it as if we never parsed it.
        goto unparsed;
      }

      // This is the same as Xid event...
      if (group_state != IN_TRANSACTION) {
        LOG(ERROR) << "Incorrect group state when receiving QueryEvent(Commit)"
                   << ", group_state: " << group_state;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCORRECT_GROUP_STATE);
        return -1;
      }
      group_state = END_OF_GROUP;
      break;
    }
    default:
    unparsed:
      if (group_state == STANDALONE) {
        group_state = END_OF_GROUP;
      }
      break;
  }

  if (group_state == END_OF_GROUP) {
    latest_completed_gtid_position = latest_event_end_position;
    latest_completed_gtid_master_position = latest_master_position;
    latest_completed_gtid = latest_start_gtid;
    if (!gtid_start_position.Update(latest_completed_gtid)) {
      LOG(ERROR) << "Failed to update binlog start position with "
                 << latest_completed_gtid.ToString()
                 << "(start pos: " << gtid_start_position.ToString() << ")";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_UPDATE_START_POS);
      return -1;
    }
    group_state = NO_GROUP;
    return 1;
  }

  if (group_state == NO_GROUP) {
    latest_completed_gtid_position = latest_event_end_position;
    latest_completed_gtid_master_position = latest_master_position;
    return 1;
  }

  return 0;
}

// Is there an transaction ongoing.
bool BinlogPosition::InTransaction() const {
  return group_state != NO_GROUP;
}

std::string BinlogPosition::ToString() const {
  std::string tmp = "[ ";
  tmp += " group: " + std::to_string(static_cast<int>(group_state));
  tmp += " completed/started gtid: ";
  tmp += latest_completed_gtid.ToString();
  tmp += "/";
  tmp += latest_start_gtid.ToString();
  tmp += " group/end position: ";
  tmp += latest_completed_gtid_position.ToString();
  tmp += "/";
  tmp += latest_event_end_position.ToString();
  tmp += " master/next position: ";
  tmp += latest_master_position.ToString();
  tmp += "/";
  tmp += next_master_position.ToString();
  tmp += " ]";
  return tmp;
}

}  // namespace mysql_ripple
