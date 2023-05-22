#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>

#include "include/rs/builder.h"
#include "include/rs/radix_spline.h"
#include "include/delta_index/helper.h"
#include "include/delta_index/xindex_buffer_impl.h"

template <class KeyType>
class RSPlus{

 public:
    RSPlus() = delete;                  
    RSPlus(std::vector<KeyType> * k);

 private:
    rs::RadixSpline<KeyType> active_learned_index; //LearnedIndex to which reads are directed
    xindex::AltBtreeBuffer<KeyType, KeyType> * active_delta_index; //DeltaIndex to which writes are directed

};

template <class KeyType>
RSPlus<KeyType>::RSPlus(std::vector<KeyType> * keys){
    //Assumption: keys is a sorted vector

    //Extract minimum and maximum value of the data you want to approximate with the spline
    KeyType min_key = keys->front();
    KeyType max_key = keys->back();

    //Construct the spline in a single pass by iterating over the keys
    rs::Builder<KeyType> rsb(min_key, max_key);
    for (const auto& key : *keys) rsb.AddKey(key); //TODO: check this for performance
    active_learned_index = rsb.Finalize();

    //Create a new empty delta index
    active_delta_index = new xindex::AltBtreeBuffer<KeyType, KeyType>();
}

#endif