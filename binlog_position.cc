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
  // the format descriptor event consists of some fixed size parts
  // and then a variable section and finally the checksum TRUE/FALSE in the end.
  // we don't (currently) care about the variable section.
  // const int fixed_len = 2 + kServerVersionStringLen + 4 + 1;
  // if (len < fixed_len) {
  //   return false;
  // }
  uint64_t table_id;
  std::string schema_name;
  std::string table_name;


  table_id = byte_order::load6(buffer);

  uint8_t schema_name_len = byte_order::load1(buffer + 8);
  schema_name.assign(reinterpret_cast<const char*>(buffer + 8), schema_name_len);

  uint8_t table_name_len = byte_order::load1(buffer + 8 + schema_name_len);
  table_name.assign(reinterpret_cast<const char*>(buffer + 8 + schema_name_len), table_name_len);

  LOG(INFO) << "schema_name: " << schema_name
            << " table name: " << table_name
            << ", table_id: " << std::to_string(table_id);

  // TODO: printf!

  // const char *ptr = reinterpret_cast<const char*>(buffer + 2);
  // size_t version_len = strnlen(ptr, kServerVersionStringLen);
  // server_version = std::string(ptr, version_len);
  // create_timestamp = byte_order::load4(buffer + kServerVersionStringLen + 2);
  // event_header_length = byte_order::load1(buffer + kServerVersionStringLen +
  //                                         2 + 4);
  // for (uint offset = kServerVersionStringLen + 2 + 4 + 1; offset < len - 1;
  //      offset++) {
  //   event_type_header_lengths.push_back(byte_order::load1(buffer + offset));
  // }
  // checksum = byte_order::load1(buffer + len - 1);
  // return true;
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
      // TableMapEvent ev;
      // if (!ev.ParseFromRawLogEventData(event)) {
      //   LOG(ERROR) << "Failed to parse QueryEvent";
      //   monitoring::rippled_binlog_error->Increment(
      //     monitoring::ERROR_PARSE_QUERY);
      //   return -1;
      // }
      group_state = END_OF_GROUP;
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
