//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  replacer_place_holder = new ReplacerPagePlaceHolder[num_pages];
  clock_hand = 0;
  size_of_replacer_place_holders = 0;
  num_pages_ = num_pages;
}

ClockReplacer::~ClockReplacer() = default;
bool ClockReplacer::Victim(frame_id_t *frame_id) {
  //frame_id_t least_id = -1;
  int nonexist_count =0;
  bool found = false;
  int* counted = new int[num_pages_];
  while (!found) {
    if (clock_hand >= frame_id_t(num_pages_)) {
      clock_hand = 0;
      }
    if (!replacer_place_holder[clock_hand].pinned) {
      //least_id = clock_hand;
      if (!replacer_place_holder[clock_hand].reference_bit) {
        replacer_place_holder[clock_hand].reference_bit = false;
        replacer_place_holder[clock_hand].pinned = true;
        //std::cout<<clock_hand<<std::endl;
        *frame_id = clock_hand;
        size_of_replacer_place_holders--;
        clock_hand++;
        found = true;
      } else if (replacer_place_holder[clock_hand].reference_bit) {
        replacer_place_holder[clock_hand].reference_bit = false;
        clock_hand++;
      }
    } else {
      if(!counted[clock_hand]){
      nonexist_count++;
      counted[clock_hand] = true;
      }
      clock_hand++;
      //CHANGE =,0
    }
    if(nonexist_count == int(num_pages_)){
      break;
    }
  }
  // if(!found){
  //   if (least_id != -1) {
  //     replacer_place_holder[least_id].reference_bit = false;
  //     replacer_place_holder[least_id].exist = false;
  //     *frame_id = least_id;
  //     size_of_replacer_place_holders--;
  //     found = true;
  //   }
  // }
  return found;
}
// bool ClockReplacer::Victim(frame_id_t *frame_id) {
//   int clock_hand_beginning = clock_hand;
//   frame_id_t least_id = -1;
//   bool first_loop = true;
//   bool found = false;
//   while ((clock_hand != clock_hand_beginning || first_loop) && !found) {
//     first_loop = false;
//     if (replacer_place_holder[clock_hand].exist) {
//       least_id = clock_hand;
//       if (!replacer_place_holder[clock_hand].reference_bit) {
//         replacer_place_holder[clock_hand].reference_bit = false;
//         replacer_place_holder[clock_hand].exist = false;
//         *frame_id = clock_hand;
//         size_of_replacer_place_holders--;
//         clock_hand++;
//         found = true;
//       } else if (replacer_place_holder[clock_hand].reference_bit) {
//         replacer_place_holder[clock_hand].reference_bit = false;
//         clock_hand++;
//       }
//     } else {
//       clock_hand++;
//       //CHANGE =,0
//       if (clock_hand == frame_id_t(num_pages_)) {
//         clock_hand = 0;
//       }
//     }
//   }
//   if(!found){
//     if (least_id != -1) {
//       replacer_place_holder[least_id].reference_bit = false;
//       replacer_place_holder[least_id].exist = false;
//       *frame_id = least_id;
//       size_of_replacer_place_holders--;
//       found = true;
//     }
//   }
//   return found;
// }

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (!replacer_place_holder[frame_id].pinned) {
    replacer_place_holder[frame_id].pinned = true;
    size_of_replacer_place_holders--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (replacer_place_holder[frame_id].pinned) {
    replacer_place_holder[frame_id].pinned = false;
    replacer_place_holder[frame_id].reference_bit = true;
    size_of_replacer_place_holders++;
  }
}

size_t ClockReplacer::Size() { return size_of_replacer_place_holders; }

}  // namespace bustub
