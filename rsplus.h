#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>
#include <utility>
#include <iostream>
#include <mutex>

#include "learnedindex.h"
#include "deltaindex.h"

template <class KeyType, class ValueType>
class RSPlus{

 public:
    RSPlus() = delete;                  
    RSPlus(std::vector<std::pair<KeyType, ValueType>> & k);

    bool find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag);
    void insert(const KeyType &lookup_key, const ValueType &val);
    void remove(const KeyType &lookup_key);    
    void compact();
    
 private:   
    LearnedIndex<KeyType, ValueType> * active_learned_index;//L earnedIndex to which reads are directed
    LearnedIndex<KeyType, ValueType> * next_learned_index;// LearnedIndex which is currently trained
    
    DeltaIndex<KeyType, ValueType> * active_delta_index; // DeltaIndex to which writes are directed
    DeltaIndex<KeyType, ValueType> * prev_delta_index; // read-only DeltaIndex from which we read values that are currently being flashed

    std::mutex delta_index_mutex;   // mutex that protects active_delta_index from being changed by compaction
    std::mutex learned_index_mutex; // mutex that protects active_learned_index from being changed by compaction
};

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::vector<std::pair<KeyType, ValueType>> & k){
    // Assumption: keys is a non-empty vector sorted with regard to keys

    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k);
    next_learned_index = nullptr;

    // Create a new empty delta index to keep changes
    active_delta_index = new DeltaIndex<KeyType, ValueType>();
    prev_delta_index = nullptr;   
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag){
    // Finds the exact key in the delta index, if it exists. Returns true if the key exists, false otherwise.
    // If the function returns false, then the value of &val is undefined

    // Initially we have not found the key
    bool key_found = false;

    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase
    delta_index_mutex.lock();

    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;  
    
    current_delta_index->readers_in++; 
    if(frozen_delta_index) frozen_delta_index->readers_in++;

    delta_index_mutex.unlock();
    
    // Search for the key in the active delta index
    key_found = current_delta_index->find(lookup_key, val, deleted_flag);

    // If a frozen_delta_index is available
    if(frozen_delta_index){
        // If no key could be found in the current dela, do an additional lookup at in the previous delta
        if(!key_found) key_found = frozen_delta_index->find(lookup_key, val, deleted_flag);
        frozen_delta_index->readers_out++; // atomic because we are out of the critical section
    }

    current_delta_index->readers_out++; // atomic because we are out of the critical section

    // If no key could be found in the deltas, 
    if(!key_found){
        int temp_offset;
        deleted_flag = false;   // no entry in delta => no changes => no deletion

        // Get reference to the learned index. Compaction cannot change them while we hold the lock.
        // mutex => no concurrent increases => no need for atomic increase
        learned_index_mutex.lock();
        LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;
        current_learned_index->readers_in++; 
        learned_index_mutex.unlock();

        key_found = current_learned_index->find(lookup_key, temp_offset, val);
        current_learned_index->readers_out++; // atomic because we are out of the critical section
    }

    return key_found;

}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::insert(const KeyType &lookup_key, const ValueType &val){
   
    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase   
    delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * current_delta_index = active_delta_index;
    current_delta_index->writers_in++;
    delta_index_mutex.unlock();

    current_delta_index->insert(lookup_key, val);
    current_delta_index->writers_out++; // atomic because we are out of the critical section
}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::remove(const KeyType &lookup_key){

    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase   
    delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * current_delta_index = active_delta_index;
    current_delta_index->writers_in++;
    delta_index_mutex.unlock();

    current_delta_index->remove(lookup_key);
    current_delta_index->writers_out++; // atomic because we are out of the critical section
}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::compact(){
    std::cout << "Not implemented yet." << std::endl;
}

#endif

// TODO: think about different lock for delta-write and different for delta-read (compaction must take both)
// TODO: think about different lock for active-delta and different for prev-delta (compaction must take both)
// NOTE: maybe disable assertions for experiments