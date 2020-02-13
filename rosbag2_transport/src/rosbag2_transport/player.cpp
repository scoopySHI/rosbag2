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

void Player::play_messages_until_queue_empty()
{
  ReplayableMessage message;
  while (message_queue_.try_dequeue(message) && rclcpp::ok()) {
    std::this_thread::sleep_until(start_time_ + message.time_since_start);

    if ( topics_ts_map_.find(message.message->topic_name) != topics_ts_map_.end() ) {
      builtin_interfaces::msg::Time ros_time_to_set;
      //auto time_ptr = std::make_shared<builtin_interfaces::msg::Time>(ros_time_to_set);

      std::chrono::time_point<std::chrono::system_clock> time_to_be_set = start_time_ + message.time_since_start;
      auto offset_to_be_set = topics_ts_map_[message.message->topic_name];
      //print offset to struct
      std::cout << "offset in struct: " << offset_to_be_set << "\n";
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
      buffer_temp = buffer_temp + 4 + offset_to_be_set/2;
      memcpy(buffer_temp, &ros_time_to_set, sizeof(builtin_interfaces::msg::Time));
      hexdump(message.message->serialized_data->buffer, sizeof(builtin_interfaces::msg::Time) + 24);
      std::cout << "--------------------------------------------\n";
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

//void Player::prepare_clock(ReplayableMessage & message)
//{
//  if(message.message->topic_name == "/imu/data")
//  {
//    std::cout << "change time stamper" << "\n";
//    auto topic_msg = std::make_shared<sensor_msgs::msg::Imu>();
//    auto string_ts = rosidl_typesupport_cpp::get_message_type_support_handle<sensor_msgs::msg::Imu>();
//    auto ret_de = rmw_deserialize(message.message->serialized_data.get(), string_ts, topic_msg.get());
//    if (ret_de != RMW_RET_OK) {
//              fprintf(stderr, "failed to deserialize serialized message\n");
//              return;
//    }

//    rclcpp::Clock Clock_now;
//    builtin_interfaces::msg::Time current_time = Clock_now.now();
//    topic_msg->header.stamp = current_time;

//    auto ret_se = rmw_serialize(topic_msg.get(), string_ts, message.message->serialized_data.get());
//    if (ret_se != RMW_RET_OK) {
//              fprintf(stderr, "failed to serialize serialized message\n");
//              return;
//    }

//    std::cout << current_time.sec << "\n";
//  }
//}

void Player::prepare_topic_ts_map()
{
  auto topics = reader_->get_all_topics_and_types();

  //build the map
  for (const auto & topic : topics){
      auto type_support = rosbag2::get_typesupport(topic.type, "rosidl_typesupport_introspection_cpp");
      auto msg_members = static_cast<const rosidl_typesupport_introspection_cpp::MessageMembers *>(type_support->data);

      const rosidl_typesupport_introspection_cpp::MessageMember *msg_member_ptr = msg_members->members_;
      auto msg_member_count = msg_members->member_count_;
      for (unsigned int i = 0;i < msg_member_count;i++) {
          if(strcmp(msg_member_ptr[i].name_, "header") == 0)
          {
              topics_ts_map_.insert(std::make_pair(topic.name, msg_member_ptr[i].offset_));
              break;
          }
      }
  }

  //check map
  for(auto it = topics_ts_map_.begin(); it != topics_ts_map_.end(); it++)
  {
    std::cout << "Name: " << it->first << "    header offset: " << it->second << "\n";
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
