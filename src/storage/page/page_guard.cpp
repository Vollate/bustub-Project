#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), useless_(that.useless_) {
  that.useless_ = true;
}

void BasicPageGuard::Drop() {
  useless_ = true;
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
}

BasicPageGuard::~BasicPageGuard() {
  if (!useless_) {
    Drop();
  }
};  // NOLINT

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (!useless_) {
    Drop();
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  useless_ = that.useless_;
  that.useless_ = true;
  return *this;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  that.guard_.useless_ = true;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (!guard_.useless_) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  that.guard_.useless_ = true;
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (!guard_.useless_) {
    Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  that.guard_.useless_ = true;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (!guard_.useless_) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  that.guard_.useless_ = true;
  return *this;
}

void WritePageGuard::Drop() {
  if (!guard_.useless_) {
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {
  if (!guard_.useless_) {
    Drop();
  }
}  // NOLINT

}  // namespace bustub
