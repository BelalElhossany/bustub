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

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    replacer_place_holder = new ReplacerPagePlaceHolder[num_pages];
    clock_hand = 0;
    size_of_replacer_place_holders=0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    int clock_hand_beginning=clock_hand;
    int least_id = -1;
    bool first_loop = true;
    while(clock_hand != clock_hand_beginning || first_loop)
    {
        first_loop= false;
        if(replacer_place_holder[clock_hand].exist ){
            least_id=clock_hand;
            if(!replacer_place_holder[clock_hand].reference_bit){
                replacer_place_holder[clock_hand].reference_bit=false;
                replacer_place_holder[clock_hand].exist=false;
                frame_id=clock_hand;
                return true;
            }
            else if(replacer_place_holder[clock_hand].reference_bit)
            {
                replacer_place_holder[clock_hand].reference_bit = false;
            }
        }
        else{
            clock_hand++;
            if(clock_hand==frame_id){
                clock_hand = 0;
            }
        }
    }
    if(least_id == -1){
        return false;
    }
    else{
        replacer_place_holder[least_id].reference_bit=false;
        replacer_place_holder[least_id].exist=false
        frame_id= least_id;
        return true;
    }
 }

void ClockReplacer::Pin(frame_id_t frame_id) {
    replacer_place_holder[frame_id].exist = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    replacer_place_holder[frame_id].exist = true;
    replacer_place_holder[frame_id].reference_bit = true;
    size_of_replacer_place_holders++;
}

size_t ClockReplacer::Size() { return size_of_replacer_place_holders; }

}  // namespace bustub
