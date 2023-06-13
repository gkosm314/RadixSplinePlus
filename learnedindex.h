#ifndef LEARNED_INDEX_HEADER
#define LEARNED_INDEX_HEADER

#include <atomic>

#include "include/rs/builder.h"
#include "include/rs/builderwithoutminmax.h"
#include "include/rs/radix_spline.h"

template <class KeyType, class ValueType>
class LearnedIndex{
 public:
    // constructor1 - you fill the builder from a vector of pairs
    LearnedIndex(std::vector<std::pair<KeyType, ValueType>> * k, size_t num_radix_bits = 18, size_t max_error = 32);
    // constructor2 - you fill the builder from an array of pairs
    LearnedIndex(std::pair<KeyType, ValueType> * kv_array, size_t num, size_t num_radix_bits = 18, size_t max_error = 32);
    // constructor3 - builder is already filled for you
    LearnedIndex(std::vector<std::pair<KeyType, ValueType>> * k, rs::BuilderWithoutMinMax<KeyType> & rsb); 
    // destructor
    ~LearnedIndex();
    
    inline std::size_t length();
    bool lookup(const KeyType &lookup_key, int &offset); // get offset of ">=" key
    inline bool find(const KeyType &lookup_key, int &offset, bool &deleted_flag); // get offset of "==" key
    inline bool find(const KeyType &lookup_key, int &offset, ValueType &val, bool &deleted_flag); // get offset of "==" key and associated value
    inline bool update(const int &position, const ValueType &val);
    inline bool remove(const int &position);
    
    KeyType min_key, max_key;
    inline bool empty() const;
    inline typename std::vector<std::pair<KeyType, ValueType>>::iterator begin() const;
    inline typename std::vector<std::pair<KeyType, ValueType>>::iterator end() const;
    inline bool get_is_removed(typename std::vector<std::pair<KeyType, ValueType>>::iterator & iter) const;
    
 private:
    std::vector<std::pair<KeyType, ValueType>> * kv_data; // The key-value store over which the active_learned_index approximates.
    std::vector<bool> * is_removed; // We use vector<bool> instead of a triplet because of the space-efficient implementation 

    rs::RadixSpline<KeyType> rspline;
};

template <class KeyType, class ValueType>
LearnedIndex<KeyType, ValueType>::LearnedIndex(std::vector<std::pair<KeyType, ValueType>> * k, size_t num_radix_bits, size_t max_error){

    // Keys should be pointing to the initial data
    kv_data = k;

    // Initialize is_removed so that no vector is removed initially
    is_removed = new std::vector<bool>(kv_data->size(), false);
    
    if(!(*k).empty()){
        // Extract minimum and maximum value of the data you want to approximate with the spline
        min_key = kv_data->front().first;
        max_key = kv_data->back().first;
    }
    else{
        min_key = std::numeric_limits<KeyType>::min();
        max_key = std::numeric_limits<KeyType>::min();
    }

    // Construct the spline in a single pass by iterating over the keys
    rs::Builder<KeyType> rsb(min_key, max_key, num_radix_bits, max_error);
    for (const auto& kv_pair : *k) rsb.AddKey(kv_pair.first);
    rspline = rsb.Finalize();
}

template <class KeyType, class ValueType>
LearnedIndex<KeyType, ValueType>::LearnedIndex(std::pair<KeyType, ValueType> * kv_array, size_t num, size_t num_radix_bits, size_t max_error){

    // Create empty data vector and reserve space
    kv_data = new std::vector<std::pair<KeyType, ValueType>>;
    kv_data->reserve(num);

    // Initialize is_removed so that no vector is removed initially
    is_removed = new std::vector<bool>(num, false);

    if(num > 0){
        // Extract minimum and maximum value of the data you want to approximate with the spline
        min_key = kv_array[0].first;
        max_key = kv_array[num-1].first;
    }
    else{
        min_key = std::numeric_limits<KeyType>::min();
        max_key = std::numeric_limits<KeyType>::min();
    }

    // Construct the spline in a single pass by iterating over the keys
    rs::Builder<KeyType> rsb(min_key, max_key, num_radix_bits, max_error);
    for (int i = 0; i < num; i++) {
        kv_data->push_back(kv_array[i]);
        rsb.AddKey(kv_array[i].first);
    }
    rspline = rsb.Finalize();

}

template <class KeyType, class ValueType>
LearnedIndex<KeyType, ValueType>::LearnedIndex(std::vector<std::pair<KeyType, ValueType>> * k, rs::BuilderWithoutMinMax<KeyType> & rsb){
    
    // Keys should be pointing to the initial data
    kv_data = k;
    
    // Initialize is_removed so that no vector is removed initially
    is_removed = new std::vector<bool>(kv_data->size(), false);

    // Construct spline by finalizing the builder that was passed as a parameter
    // Nothing changes if the builder is empty
    rspline = rsb.Finalize();
}

template <class KeyType, class ValueType>
LearnedIndex<KeyType, ValueType>::~LearnedIndex(){
    delete kv_data;
    delete is_removed;
}

template <class KeyType, class ValueType>
inline std::size_t LearnedIndex<KeyType, ValueType>::length(){
    return kv_data->size();
}

template <class KeyType, class ValueType>
bool LearnedIndex<KeyType, ValueType>::lookup(const KeyType &lookup_key, int &offset){
    // Finds the next smallest number in keys just greater than or equal to that number and stores it in offset
    // Returns false if such number does not exist, true otherwise
    // Note: offset will be out-of-bounds for the keys vector when the function returns false

    // Search bound for local search using RadixSpline
    rs::SearchBound bound = rspline.GetSearchBound(lookup_key);
    
    // Perform binary search inside the error bounds to find the exact position
    auto start = std::begin(*kv_data) + bound.begin, last = std::begin(*kv_data) + bound.end;
    auto binary_search_offset = std::lower_bound(start, last, lookup_key,
                    [](const std::pair<KeyType, ValueType>& lhs, const KeyType& rhs){
                        return lhs.first < rhs;
                    });
    offset = binary_search_offset - std::begin(*kv_data);

    // Return true iff records greater than or equal to the given key exist in the data
    return (offset < kv_data->size());
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::find(const KeyType &lookup_key, int &offset, bool &deleted_flag){
    // Finds the exact key, if it exists. Returns true if the key exists, false otherwise.
    // Uses lookup() and stores the smallest key that is greater 
    // Note: offset could be out-of-bounds for the keys vector when the function returns false

    bool keys_greater_or_equal_exist = lookup(lookup_key, offset);

    if(keys_greater_or_equal_exist && (*kv_data)[offset].first == lookup_key){
        deleted_flag = (*is_removed)[offset];
        return true;
    }
    else return false;
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::find(const KeyType &lookup_key, int &offset, ValueType &val, bool &deleted_flag){
    // Finds the exact key, if it exists. Returns true if the key exists, false otherwise.
    // Uses lookup() and stores the smallest key that is greater 
    // Note: This implementation also returns the value associated with the given key
    // Note: offset could be out-of-bounds for the keys vector when the function returns false

    bool keys_greater_or_equal_exist = lookup(lookup_key, offset);

    if(keys_greater_or_equal_exist && (*kv_data)[offset].first == lookup_key){
        val = (*kv_data)[offset].second;
        deleted_flag = (*is_removed)[offset];
        return true;
    }
    else return false;
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::update(const int &position, const ValueType &val){
    (*kv_data)[position].second = val;
    return !(*is_removed)[position];   // if the kv pair is removed, then return false
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::remove(const int &position){
    bool res = !(*is_removed)[position];   // we want to return false if we are asked to remove an already removed record
    (*is_removed)[position] = true;
    return res;
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::empty() const{
    return kv_data->empty();
}

template <class KeyType, class ValueType>
inline typename std::vector<std::pair<KeyType, ValueType>>::iterator LearnedIndex<KeyType, ValueType>::begin() const{
    return kv_data->begin();
}

template <class KeyType, class ValueType>
inline typename std::vector<std::pair<KeyType, ValueType>>::iterator LearnedIndex<KeyType, ValueType>::end() const{
    return kv_data->end();
}

template <class KeyType, class ValueType>
inline bool LearnedIndex<KeyType, ValueType>::get_is_removed(typename std::vector<std::pair<KeyType, ValueType>>::iterator & iter) const{
    size_t offset = iter - kv_data->begin(); // the iter is from kv_data vector -> decrease the begin() iter of this vector to get a valid offset
    return (*is_removed)[offset];
}

#endif