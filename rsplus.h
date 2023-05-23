#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>
#include <iostream>

#include "include/rs/builder.h"
#include "include/rs/radix_spline.h"
#include "include/delta_index/helper.h"
#include "include/delta_index/xindex_buffer_impl.h"

template <class KeyType>
class RSPlus{

 public:
    RSPlus() = delete;                  
    RSPlus(std::vector<KeyType> & k);

    bool lookup_learned_index(const KeyType &lookup_key, int &res); //Function used to lookup the active_learned_index
    bool find_learned_index(const KeyType &lookup_key, int &res); //Function used to find elements in the active_learned_index

 private:
    std::vector<KeyType> * keys; //The keys that the active_learned_index approximates.

    rs::RadixSpline<KeyType> active_learned_index; //LearnedIndex to which reads are directed
    xindex::AltBtreeBuffer<KeyType, KeyType> * active_delta_index; //DeltaIndex to which writes are directed
};

template <class KeyType>
RSPlus<KeyType>::RSPlus(std::vector<KeyType> & k){
    //Assumption: keys is a non-empty sorted vector

    //Keys should be pointing to the initial data
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
bool RSPlus<KeyType>::lookup_learned_index(const KeyType &lookup_key, int &res){
    //Finds the next smallest number in keys just greater than or equal to that number and stores it in res
    //Returns false if such number does not exist, true otherwise
    //Note: res will be out-of-bounds for the keys vector when the function returns false

    //TODO: check return type of this function
    //TODO: check dereference for performance penalty

    //Search bound for local search using RadixSpline
    rs::SearchBound bound = active_learned_index.GetSearchBound(lookup_key);
    
    //Perform binary search inside the error bounds to find the exact position
    auto start = begin(*keys) + bound.begin, last = begin(*keys) + bound.end;
    res = std::lower_bound(start, last, lookup_key) - begin(*keys);

    //Return true iff records greater than or equal to the given key exist in the data
    return (res < keys->size());
}

template <class KeyType>
bool RSPlus<KeyType>::find_learned_index(const KeyType &lookup_key, int &res){
    //Finds the exact key, if it exists. Returns true if the key exists, false otherwise.
    //Uses lookup_learned_index() and stores the smallest key that is greater 
    //Note: res could be out-of-bounds for the keys vector when the function returns false

    bool keys_greater_or_equal_exist = lookup_learned_index(lookup_key, res);

    if(keys_greater_or_equal_exist && (*keys)[res] == lookup_key) return true;
    else return false;
}

#endif