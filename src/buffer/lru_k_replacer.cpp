//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {
// LRUKNode private

// LRUKNode public
LRUKNode::LRUKNode(size_t k, frame_id_t frame_id) : k_(k), fid_(frame_id) {}

void LRUKNode::SetEvictable(bool flag) { is_evictable_ = flag; }

auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }

// void LRUKNode::ClearHistory() {
// history_.clear();
// UpdateSizeInfo();
//}

auto LRUKNode::RecordAccess(frame_id_t frame_id, size_t timestamp) -> bool {
  fid_ = frame_id;
  bool is_new_node = history_.empty();
  history_.push_front(timestamp);
  if (is_full_) {
    history_.pop_back();
  } else {
    UpdateSizeInfo();
  }
  return is_new_node;
}

// LRUKReplacer public
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    return false;
  }
  auto &&history_index =
      std::find_if(history_.rbegin(), history_.rend(), [](const auto &node) { return node->IsEvictable(); });
  if (history_index == history_.rend()) {
    auto &&buffer_index =
        std::find_if(buffer_.rbegin(), buffer_.rend(), [](const auto &node) { return node->IsEvictable(); });
    *frame_id = RemoveNode(*buffer_index);
  } else {
    *frame_id = RemoveNode(*history_index);
  }
  return true;
}
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard guard{latch_};
  RecordAccessImplement(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard guard{latch_};
  if (node_store_[frame_id].IsEvictable() && !set_evictable) {
    --curr_size_;
  } else if (!node_store_[frame_id].IsEvictable() && set_evictable) {
    ++curr_size_;
  }
  node_store_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard guard{latch_};
  if (!node_store_[frame_id].IsEvictable()) {
    throw std::runtime_error("Try to remove a non-evictable frame");
  }
  RemoveNode(&node_store_[frame_id]);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock{latch_};
  return curr_size_;
}

// LRUKReplacer private
void LRUKReplacer::UpdateCurrentTimeStamp() {
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
}

auto LRUKReplacer::RemoveNode(LRUKNode *node) -> frame_id_t {
  auto id = node->GetFrameId();
  const auto &node_info = node_location_[id];
  switch (node_info.first) {
    case Location::History:
      history_.erase(node_info.second);
      break;
    default:
      buffer_.erase(node_info.second);
  }
  node_location_.erase(id);
  node_store_.erase(id);
  --curr_size_;
  return id;
}

void LRUKReplacer::RecordAccessImplement(frame_id_t frame_id) {
  UpdateCurrentTimeStamp();
  if (node_store_[frame_id].RecordAccess(frame_id, current_timestamp_)) {  // new node
    if (node_store_.size() > replacer_size_) {
      node_store_.erase(frame_id);
      throw std::runtime_error("Meet the capacity");
    }
    auto &new_node = node_store_[frame_id];
    if (new_node.ShouldStoreInBuffer()) {
      buffer_.push_front(&new_node);
      node_location_[frame_id] = {Location::Buffer, buffer_.begin()};
    } else {
      history_.push_front(&new_node);
      node_location_[frame_id] = {Location::History, history_.begin()};
    }
  } else {  // exist node
    if (auto &store_info = node_location_[frame_id];
        store_info.first == Location::History && node_store_[frame_id].ShouldStoreInBuffer()) {
      history_.erase(store_info.second);
      buffer_.push_front(&node_store_[frame_id]);
      store_info = {Location::Buffer, buffer_.begin()};
    } else {
      buffer_.erase(store_info.second);
      buffer_.push_front(&node_store_[frame_id]);
      store_info.second = buffer_.begin();
    }
  }
}
}  // namespace bustub
