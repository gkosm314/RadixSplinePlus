#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>
#include <utility>
#include <iostream>

#include "include/rs/builder.h"
#include "include/rs/radix_spline.h"
#include "include/delta_index/helper.h"
#include "include/delta_index/xindex_buffer_impl.h"

template <class KeyType, class ValueType>
class RSPlus{

 public:
    RSPlus() = delete;                  
    RSPlus(std::vector<std::pair<KeyType, ValueType>> & k);
    
    // TODO: move these methods to private
    bool lookup_learned_index(const KeyType &lookup_key, int &res) const; // lookup active learned index, get offset of ">=" key
    bool find_learned_index(const KeyType &lookup_key, int &res) const; // lookup active learned index, get offset of "==" key
    bool find_learned_index(const KeyType &lookup_key, int &res, ValueType &val) const; // lookup active learned index, get offset of "==" key + associated value
    
    bool find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag) const; // lookup active learned index, get value of "==" key + delete status of key
    void insert_delta_index(const KeyType &lookup_key, const ValueType &val) const; // insert key-value pair in the delta index
    void delete_delta_index(const KeyType &lookup_key) const; // delete key from the delta index

 private:
    std::vector<std::pair<KeyType, ValueType>> * kv_data; //The key-value store over which the active_learned_index approximates.

    rs::RadixSpline<KeyType> active_learned_index; //LearnedIndex to which reads are directed
    xindex::AltBtreeBuffer<KeyType, ValueType> * active_delta_index; //DeltaIndex to which writes are directed
};

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::vector<std::pair<KeyType, ValueType>> & k){
    // Assumption: keys is a non-empty vector sorted with regard to keys

    // Keys should be pointing to the initial data
    kv_data = &k;
    
    // Extract minimum and maximum value of the data you want to approximate with the spline
    KeyType min_key = kv_data->front().first;
    KeyType max_key = kv_data->back().first;

    // Construct the spline in a single pass by iterating over the keys
    rs::Builder<KeyType> rsb(min_key, max_key);
    for (const auto& kv_pair : k) rsb.AddKey(kv_pair.first);
    active_learned_index = rsb.Finalize();

    // Create a new empty delta index
    active_delta_index = new xindex::AltBtreeBuffer<KeyType, ValueType>();
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::lookup_learned_index(const KeyType &lookup_key, int &res) const{
    // Finds the next smallest number in keys just greater than or equal to that number and stores it in res
    // Returns false if such number does not exist, true otherwise
    // Note: res will be out-of-bounds for the keys vector when the function returns false

    // TODO: check return type of this function
    // TODO: check dereference for performance penalty

    // Search bound for local search using RadixSpline
    rs::SearchBound bound = active_learned_index.GetSearchBound(lookup_key);
    
    // Perform binary search inside the error bounds to find the exact position
    auto start = begin(*kv_data) + bound.begin, last = begin(*kv_data) + bound.end;
    auto binary_search_res = std::lower_bound(start, last, lookup_key,
                    [](const std::pair<KeyType, ValueType>& lhs, const KeyType& rhs){
                        return lhs.first < rhs;
                    });
    res = binary_search_res - begin(*kv_data);

    // Return true iff records greater than or equal to the given key exist in the data
    return (res < kv_data->size());
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_learned_index(const KeyType &lookup_key, int &res) const{
    // Finds the exact key, if it exists. Returns true if the key exists, false otherwise.
    // Uses lookup_learned_index() and stores the smallest key that is greater 
    // Note: res could be out-of-bounds for the keys vector when the function returns false

    bool keys_greater_or_equal_exist = lookup_learned_index(lookup_key, res);

    if(keys_greater_or_equal_exist && (*kv_data)[res].first == lookup_key) return true;
    else return false;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_learned_index(const KeyType &lookup_key, int &res, ValueType &val) const{
    // Finds the exact key, if it exists. Returns true if the key exists, false otherwise.
    // Uses lookup_learned_index() and stores the smallest key that is greater 
    // Note: This implementation also returns the value associated with the given key
    // Note: res could be out-of-bounds for the keys vector when the function returns false

    bool keys_greater_or_equal_exist = lookup_learned_index(lookup_key, res);

    if(keys_greater_or_equal_exist && (*kv_data)[res].first == lookup_key){
        val = (*kv_data)[res].second;
        return true;
    }
    else return false;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag) const{
    // Finds the exact key in the delta index, if it exists. Returns true if the key exists, false otherwise.
    // Attention: If the key does not exist, then &val and &deleted_flag are not changed.
    return active_delta_index->get(lookup_key, val, deleted_flag);
}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::insert_delta_index(const KeyType &lookup_key, const ValueType &val) const{
    // Delete given key-value pair in the delta index.
    // Even though we pass by reference, internally the value is inserted in the buffer, not the ref
    active_delta_index->insert(lookup_key, val); 
}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::delete_delta_index(const KeyType &lookup_key) const{
    // Delete given key from delta index.
    // If the key exists, it is marked as deleted, otherwise we insert it and then mark it as deleted.
    active_delta_index->remove(lookup_key);
}

#endif