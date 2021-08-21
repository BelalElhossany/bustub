//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  Page* found_page = nullptr;
  char* data_= new char[PAGE_SIZE];
  
  auto page_table_object =page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if(page_table_object != page_table_.end()) {
    replacer_->Pin(page_table_object->second);
    found_page = &pages_[page_table_object->second];
    pages_[page_table_object->second].pin_count_++;
  }
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
  else{
    int free_list_slot = -1;
    int page_to_replace = -1;
    if(free_list_.size() != 0){
      free_list_slot = free_list_.front();
      free_list_.pop_front();
      disk_manager_->ReadPage(page_id,data_);
      std::memcpy(pages_[free_list_slot].GetData(), &data_, PAGE_SIZE);
      pages_[free_list_slot].page_id_=page_id;
      found_page= &pages_[free_list_slot];
      pages_[free_list_slot].pin_count_++;
      page_table_[page_id]= free_list_slot;
      // Add to replacer
      replacer_->Pin(free_list_slot);
    }
    else{
        int frame_to_replace = INT_MAX;
        replacer_->Victim(&frame_to_replace);
        if(frame_to_replace < int(pool_size_))
        {
          for(auto iter = page_table_.begin(); iter != page_table_.end(); ++iter){
              if(iter->second == frame_to_replace)
              {
                page_to_replace = iter->first;
                page_table_.erase(iter);
                break;
              }

          }
          // Add to replacer
          replacer_->Pin(frame_to_replace);
          // 2.     If R is dirty, write it back to the disk.
          if(pages_[frame_to_replace].is_dirty_){
                disk_manager_->WritePage(page_to_replace,pages_[frame_to_replace].GetData());
              }
          // 3.     Delete R from the page table and insert P.
          page_table_[page_id]=frame_to_replace;

          // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
          disk_manager_->ReadPage(page_id,data_);
          //std::cout<<data_<<std::endl;
          std::memcpy(pages_[frame_to_replace].GetData(), data_, PAGE_SIZE);
          pages_[frame_to_replace].is_dirty_=false;
          pages_[frame_to_replace].page_id_=page_id;
          found_page = &pages_[frame_to_replace];
          pages_[frame_to_replace].pin_count_++;

        }

    }

  }
    return found_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  bool success=true;
  auto page_table_object =page_table_.find(page_id);
  if(pages_[page_table_object->second].pin_count_ <= 0){
    success = false;
  }
  if(success == true){
    pages_[page_table_object->second].pin_count_--;
    replacer_->Unpin(page_table_object->second);
    // std::cout<<page_table_object->second << std::endl;
    pages_[page_table_object->second].is_dirty_=is_dirty;
    // int frame_to_replace;
    // replacer_->Victim(&frame_to_replace);
    // std::cout<<replacer_->Size() <<" "<<frame_to_replace<<std::endl;
  }
   return success;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  bool success;
  if(page_id == INVALID_PAGE_ID){
    success=false;
  }
  else{
    auto page_table_object =page_table_.find(page_id);
    if (page_table_object != page_table_.end()){
      disk_manager_->WritePage(page_id,pages_[page_table_object->second].GetData());
      success=true;
    }
    else{
      success=false;
    }
  }
  // Make sure you call DiskManager::WritePage!
  return success;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  *page_id = disk_manager_->AllocatePage(); 
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  Page* returned_page=nullptr;
  bool notpinned_available=false;
  for(int i=0 ; i<int(pool_size_); i++)
  {
    if(pages_[i].pin_count_ == 0)
    {
      notpinned_available= true;
      break;
    }
  }
  if(notpinned_available)
  {

    int free_list_slot = -1;
    int page_to_replace = -1;
    if(free_list_.size() != 0){
      free_list_slot = free_list_.front();
      free_list_.pop_front();
      pages_[free_list_slot].page_id_=*page_id;
      returned_page= &pages_[free_list_slot];
      page_table_[*page_id]= free_list_slot;
      pages_[free_list_slot].pin_count_++;
      // Add to replacer
      replacer_->Pin(free_list_slot);
    }
    else{
      frame_id_t frame_to_replace;
      replacer_->Victim(&frame_to_replace);
      for(auto iter = page_table_.begin(); iter != page_table_.end(); ++iter){
          if(iter->second == frame_to_replace)
          {
            page_to_replace = iter->first;
            page_table_.erase(iter);
            break;
          }

      }
      // Add to replacer
      replacer_->Pin(frame_to_replace);
      // 2.     If R is dirty, write it back to the disk.
      if(pages_[frame_to_replace].is_dirty_){
            disk_manager_->WritePage(page_to_replace,pages_[frame_to_replace].GetData());
          }
      // 3.     Delete R from the page table and insert P.
      page_table_[*page_id]=frame_to_replace;

      // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
      pages_[frame_to_replace].is_dirty_=false;
      pages_[frame_to_replace].page_id_=*page_id;
      pages_[frame_to_replace].pin_count_++;
      returned_page = &pages_[frame_to_replace];
    }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  }
  return returned_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  bool success=false;
  auto page_table_object =page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if(page_table_object == page_table_.end()){
    success=true;
  }
  else{
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if(pages_[page_table_object->second].pin_count_ == 0){
      success=true;
    }
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(success){
    page_table_.erase(page_table_object);
    free_list_.emplace_back(page_table_object->second);
    pages_[page_table_object->second].page_id_ = INVALID_PAGE_ID;
    pages_[page_table_object->second].is_dirty_=false;
    pages_[page_table_object->second].pin_count_=0;
  }
  return success;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for(int i=0; i< int(pool_size_);i++){
    if(pages_[i].page_id_ != INVALID_PAGE_ID){
      FlushPageImpl(pages_[i].page_id_);
    }
  }
  // You can do it!
}

}  // namespace bustub