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
    RSPlus(std::vector<KeyType> & k);

 private:
    std::vector<KeyType> * keys; //The keys that the active_learned_index approximates.

    rs::RadixSpline<KeyType> active_learned_index; //LearnedIndex to which reads are directed
    xindex::AltBtreeBuffer<KeyType, KeyType> * active_delta_index; //DeltaIndex to which writes are directed

    int lookup_learned_index(const KeyType &lookup_key); //Function used to lookup the active_learned_index
};

template <class KeyType>
RSPlus<KeyType>::RSPlus(std::vector<KeyType> & k){
    //Assumption: keys is a sorted vector

    //Keys should be
    keys = &k;

    //Extract minimum and maximum value of the data you want to approximate with the spline
    KeyType min_key = keys->front();
    KeyType max_key = keys->back();

    //Construct the spline in a single pass by iterating over the keys
    rs::Builder<KeyType> rsb(min_key, max_key);
    for (const auto& key : k) rsb.AddKey(key); //TODO: check this for performance
    active_learned_index = rsb.Finalize();

    //Create a new empty delta index
    active_delta_index = new xindex::AltBtreeBuffer<KeyType, KeyType>();
}

template <class KeyType>
int RSPlus<KeyType>::lookup_learned_index(const KeyType &lookup_key){

    //TODO: check return type of this function
    //TODO: check dereference for performance penalty

    //Search bound for local search using RadixSpline
    rs::SearchBound bound = active_learned_index.GetSearchBound(lookup_key);
    //Perform binary search inside the error bounds to find the exact position
    auto start = begin(*keys) + bound.begin, last = begin(*keys) + bound.end;
    return (std::lower_bound(start, last, lookup_key) - begin(*keys));
}

#endif