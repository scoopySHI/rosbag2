// Copyright 2018, Bosch Software Innovations GmbH.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "player.hpp"

#include <chrono>
#include <memory>
#include <queue>
#include <string>
#include <vector>
#include <utility>

#include "rclcpp/rclcpp.hpp"
#include "rcl/graph.h"
#include "rcutils/time.h"

#include "rosbag2/reader.hpp"
#include "rosbag2/typesupport_helpers.hpp"
#include "rosbag2_transport/logging.hpp"
#include "rosbag2_node.hpp"
#include "replayable_message.hpp"
#include "sensor_msgs/msg/imu.hpp"

namespace rosbag2_transport
{

const std::chrono::milliseconds
Player::queue_read_wait_period_ = std::chrono::milliseconds(100);

Player::Player(
  std::shared_ptr<rosbag2::Reader> reader, std::shared_ptr<Rosbag2Node> rosbag2_transport)
: reader_(std::move(reader)), rosbag2_transport_(rosbag2_transport)
{}

bool Player::is_storage_completely_loaded() const
{
  if (storage_loading_future_.valid() &&
    storage_loading_future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
  {
    storage_loading_future_.get();
  }
  return !storage_loading_future_.valid();
}

void Player::play(const PlayOptions & options)
{
  prepare_publishers();

  if(options.clock_type == "current")
  {
    prepare_topic_ts_map();
  }

  storage_loading_future_ = std::async(std::launch::async,
      [this, options]() {load_storage_content(options);});

  wait_for_filled_queue(options);

  play_messages_from_queue();
}

void Player::wait_for_filled_queue(const PlayOptions & options) const
{
  while (
    message_queue_.size_approx() < options.read_ahead_queue_size &&
    !is_storage_completely_loaded() && rclcpp::ok())
  {
    std::this_thread::sleep_for(queue_read_wait_period_);
  }
}

void Player::load_storage_content(const PlayOptions & options)
{
  TimePoint time_first_message;

  ReplayableMessage message;
  if (reader_->has_next()) {
    message.message = reader_->read_next();
    message.time_since_start = std::chrono::nanoseconds(0);
    time_first_message = TimePoint(std::chrono::nanoseconds(message.message->time_stamp));
    message_queue_.enqueue(message);
  }

  auto queue_lower_boundary =
    static_cast<size_t>(options.read_ahead_queue_size * read_ahead_lower_bound_percentage_);
  auto queue_upper_boundary = options.read_ahead_queue_size;

  while (reader_->has_next() && rclcpp::ok()) {
    if (message_queue_.size_approx() < queue_lower_boundary) {
      enqueue_up_to_boundary(time_first_message, queue_upper_boundary);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

void Player::enqueue_up_to_boundary(const TimePoint & time_first_message, uint64_t boundary)
{
  ReplayableMessage message;
  for (size_t i = message_queue_.size_approx(); i < boundary; i++) {
    if (!reader_->has_next()) {
      break;
    }
    message.message = reader_->read_next();
    message.time_since_start =
      TimePoint(std::chrono::nanoseconds(message.message->time_stamp)) - time_first_message;

    message_queue_.enqueue(message);
  }
}

void Player::play_messages_from_queue()
{
  start_time_ = std::chrono::system_clock::now();
  do {
    play_messages_until_queue_empty();
    if (!is_storage_completely_loaded() && rclcpp::ok()) {
      ROSBAG2_TRANSPORT_LOG_WARN("Message queue starved. Messages will be delayed. Consider "
        "increasing the --read-ahead-queue-size option.");
    }
  } while (!is_storage_completely_loaded() && rclcpp::ok());
}

void hexdump(void *ptr, int buflen)
  {
    unsigned char *buf = (unsigned char*)ptr;
    int i, j;
    for (i = 0; i < buflen; i+=16)
    {
      printf("%06x: ", i);
      for (j = 0; j < 16; j++)
        if (i + j < buflen)
          printf("%02x ", buf[i + j]);
        else
          printf("   ");
      printf(" ");
      for (j = 0; j < 16; j++)
        if (i + j < buflen)
          printf("%c", isprint(buf[i + j]) ? buf[i + j] : '.');
      printf("\n");
    }
  }

//deal with string
void Player::deal_with_string(const uint8_t *dds_buffer)
{
  std::cout << "STRING current position before change: " << current_position << "\n";
  uint32_t length;
  size_t string_header = sizeof(uint32_t);//string header 4 Bytes

  unsigned long one_offset = string_header > last_data_size ? (string_header - current_position % string_header) & (string_header-1) : 0;
  memcpy(&length, (dds_buffer+current_position+4+one_offset), string_header);
  current_position = current_position + one_offset;
  last_data_size = sizeof (char);
  current_position += (string_header + length);
  std::cout << "STRING length: " << length << "\n";
  std::cout << "STRING current position--offset: " << current_position << "--" << one_offset << "\n";
}

//deal with wstring
void Player::deal_with_wstring(const uint8_t *dds_buffer)
{
  std::cout << "WSTRING current position before change: " << current_position << "\n";
  uint32_t length;
  size_t string_header = sizeof(uint32_t);//wstring header 4 Bytes

  unsigned long one_offset = string_header > last_data_size ? (string_header - current_position % string_header) & (string_header-1) : 0;
  memcpy(&length, (dds_buffer+current_position+4+one_offset), string_header);
  current_position = current_position + one_offset;
  last_data_size = sizeof(uint32_t);
  current_position += (string_header + length * (sizeof(uint32_t)));
  std::cout << "WSTRING length: " << length << "\n";
  std::cout << "WSTRING current position---offset: " << current_position << "--" << one_offset << "\n";
}

//find out the real offset of header in fastrtps serialized data
void Player::calculate_position_with_align(const uint8_t * dds_buffer_ptr, const rosidl_typesupport_introspection_cpp::MessageMember *message_member, unsigned long stop_index)
{
  unsigned long one_offset = 0;
  unsigned long data_size = 0;

  for (unsigned int i=0; i < stop_index; i++) {
    bool is_string = false;
    bool is_wstring = false;
    switch (message_member[i].type_id_) {
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOL:
        data_size = sizeof(bool);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_BYTE:
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
        data_size = sizeof (uint8_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
        data_size = sizeof (char);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT:
        data_size = sizeof (float);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT64:
        data_size = sizeof (double);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
        data_size = sizeof (int16_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
        data_size = sizeof (uint16_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
        data_size = sizeof (int32_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
        data_size = sizeof (uint32_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
        data_size = sizeof (int64_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
        data_size = sizeof (uint64_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_LONG_DOUBLE:
        data_size = 8;
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
        deal_with_string(dds_buffer_ptr);
        is_string = true;
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_WCHAR:
        data_size = sizeof(uint16_t);
        break;
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING:
        deal_with_wstring(dds_buffer_ptr);
        is_wstring = true;
        break; //wstring not finish
      case ::rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE:
        auto sub_members = static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(message_member[i].members_->data);
        std::cout << "count: " << sub_members->member_count_ << "\n";
        calculate_position_with_align(dds_buffer_ptr, sub_members->members_, sub_members->member_count_);
        continue;
      }
    if(!is_string && !is_wstring && !message_member[i].is_array_)
      {
        std::cout << "NORMAL current position before change: " << current_position << "\n";
        one_offset = data_size > last_data_size ? (data_size - current_position % data_size) & (data_size-1) : 0;
        current_position = current_position + one_offset + data_size;
        std::cout << "NORMAL current position--data size--offset: " << current_position << "--" << data_size << "--" << one_offset << "\n";
        last_data_size = data_size;
      }
    else if(!is_string && !is_wstring && message_member[i].is_array_)
      {
        for (uint j = 0;j < message_member[i].array_size_; j++) {
          std::cout << "ARRAY current position before change: " << current_position << "\n";
          one_offset = data_size > last_data_size ? (data_size - current_position % data_size) & (data_size-1) : 0;
          current_position = current_position + one_offset + data_size;
          std::cout << "ARRAY current position--data size--offset: " << current_position << "--" << data_size << "--" << one_offset << "\n";
          last_data_size = data_size;
        }
      }
    else if (is_string && !is_wstring && message_member[i].is_array_)
      {
        for (uint j = 0;j < message_member[i].array_size_ - 1; j++) {
          deal_with_string(dds_buffer_ptr);
        }
      }
    else if (is_wstring && !is_string && message_member[i].is_array_)
      {
        for (uint j = 0;j < message_member[i].array_size_ - 1; j++) {
        deal_with_wstring(dds_buffer_ptr);
      }
    }
  }

}

void Player::play_messages_until_queue_empty()
{
  ReplayableMessage message;
  while (message_queue_.try_dequeue(message) && rclcpp::ok()) {
    std::this_thread::sleep_until(start_time_ + message.time_since_start);

    if (topics_ts_map_.find(message.message->topic_name) != topics_ts_map_.end() ) {
      builtin_interfaces::msg::Time ros_time_to_set;
      //auto time_ptr = std::make_shared<builtin_interfaces::msg::Time>(ros_time_to_set)

      std::chrono::time_point<std::chrono::system_clock> time_to_be_set = start_time_ + message.time_since_start;

      auto offset_index = topics_ts_map_[message.message->topic_name].stop_index;
      const rosidl_typesupport_introspection_cpp::MessageMember * msg_ptr = topics_ts_map_[message.message->topic_name].msg_member_ptr;
      //print offset to struct
      //std::cout << "offset in struct: " << offset_to_be_set << "\n";
      std::chrono::duration_cast<std::chrono::nanoseconds>(time_to_be_set.time_since_epoch());
      //print chrono time
      std::cout << "chrono time: " << time_to_be_set.time_since_epoch().count() << "\n";
      ros_time_to_set.sec = static_cast<int32_t>(floor(time_to_be_set.time_since_epoch().count()/1e9));
      ros_time_to_set.nanosec = static_cast<uint32_t>(round(time_to_be_set.time_since_epoch().count() - ros_time_to_set.sec*1e9));

      //test
      std::cout << "int64: " << time_to_be_set.time_since_epoch().count() - ros_time_to_set.sec*1e9 << "\n";

      //print ros time
      std::cout << "sec: " << ros_time_to_set.sec << " nano: " << ros_time_to_set.nanosec << "\n";
      //hexdeump rostime
      hexdump(&ros_time_to_set, sizeof(builtin_interfaces::msg::Time));
      std::cout << "--------------------------------------------\n";

      //hexdeump buffer
      hexdump(message.message->serialized_data->buffer, sizeof(builtin_interfaces::msg::Time) + 108);
      std::cout << "--------------------------------------------\n";

      //memcpy
      uint8_t * buffer_temp = message.message->serialized_data->buffer;
      dds_buffer_ptr = message.message->serialized_data->buffer;
      calculate_position_with_align(dds_buffer_ptr, msg_ptr, offset_index);
      std::cout << "buffer_temp: " << (unsigned long)buffer_temp << "\n";
      size_t header_time_sec_size = sizeof (int32_t);
      unsigned long last_offset = header_time_sec_size > last_data_size ? (header_time_sec_size - current_position % header_time_sec_size) & (header_time_sec_size-1): 0;
      current_position += last_offset;
      buffer_temp = buffer_temp + current_position + 4; //plus dds header
      std::cout << "current position--data size--last offset: " << current_position << "--" << header_time_sec_size << "--" << last_offset << "\n";
      std::cout << "buffer_temp: " << (unsigned long)buffer_temp << "\n";

      memcpy(buffer_temp, &ros_time_to_set, sizeof(builtin_interfaces::msg::Time));
      hexdump(message.message->serialized_data->buffer, sizeof(builtin_interfaces::msg::Time) + 48);
      std::cout << "--------------------------------------------\n";

      current_position = 0;
      last_data_size = ULONG_MAX;
    }

//    if(options.clock_type == "current")
//    {
//      prepare_clock(message);
//    }

    if (rclcpp::ok()) {
      publishers_[message.message->topic_name]->publish(message.message->serialized_data);
    }
  }
}


void Player::prepare_topic_ts_map()
{
  auto topics = reader_->get_all_topics_and_types();

  //build the map
  for (const auto & topic : topics){
      auto type_support = rosbag2::get_typesupport(topic.type, "rosidl_typesupport_introspection_cpp");
      auto msg_members = static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(type_support->data);

      //test alignment
      std::cout << "alignof Messagemember : " << alignof(rosidl_typesupport_introspection_cpp::MessageMember) << "\n";

      const rosidl_typesupport_introspection_cpp::MessageMember *msg_member_ptr = msg_members->members_;
      auto msg_member_count = msg_members->member_count_;
      for (unsigned int i = 0;i < msg_member_count;i++) {
          if(strcmp(msg_member_ptr[i].name_, "header") == 0)
          {
            //caculate header time stamper offset (int32----time.sec)
//            current_position = calculate_position_with_align(msg_member_ptr, i);
//            size_t header_time_sec_size = sizeof (int32_t);
//            unsigned long one_offset = header_time_sec_size > last_data_size ? (header_time_sec_size - current_position % header_time_sec_size) : 0;
//            unsigned long real_offset = one_offset + current_position;
            //topics_ts_map_.insert(std::make_pair(topic.name, msg_member_ptr[i].offset_));
            header_support_struct header_support;
            header_support.stop_index = i;
            header_support.msg_member_ptr = msg_member_ptr;
            topics_ts_map_.insert(std::make_pair(topic.name, header_support));
            break;
          }
      }
  }

  //check map
  for(auto it = topics_ts_map_.begin(); it != topics_ts_map_.end(); it++)
  {
    std::cout << "Name: " << it->first << "    header offset: " << it->second.stop_index << "\n";
  }

}

void Player::prepare_publishers()
{
  auto topics = reader_->get_all_topics_and_types();
  for (const auto & topic : topics) {
    publishers_.insert(std::make_pair(
        topic.name, rosbag2_transport_->create_generic_publisher(topic.name, topic.type)));
  }
}

}  // namespace rosbag2_transport
