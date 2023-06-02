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
    std::mutex compaction_mutex;    // mutex that ensures that only one compaction can take place at a given time
};

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::vector<std::pair<KeyType, ValueType>> & k){
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

    return key_found && !deleted_flag;

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

    // mutex ensures only one compaction can happen at any given time
    compaction_mutex.lock();

    // Allocate memory for the new buffer outside of the critical section, to hold the locks as little as possible
    DeltaIndex<KeyType, ValueType> * new_buffer = new DeltaIndex<KeyType, ValueType>();
    
    // Grab the mutex so that no other thread reads the deltas' locations while you change them.
    delta_index_mutex.lock();
    prev_delta_index = active_delta_index;
    active_delta_index = new_buffer;
    delta_index_mutex.unlock();

    // allocate memory before waiting => give as much time as possible to the writers to finish => wait as little as possible
    // Note: length of prev_delta_index may increase by the ongoing writes, but push_backs will resize the vector's size automatically
    std::vector<std::pair<KeyType, ValueType>> * kv_new_data = new std::vector<std::pair<KeyType, ValueType>>;
    kv_new_data->reserve(active_learned_index->length() + prev_delta_index->length()); 

    // busy wait - TODO: maybe replace with conditional variable
    while(prev_delta_index->writers_in < prev_delta_index->writers_out){}
    // We suppose that no changes happen to the prev_delta_index after this point
    
    // Grab iterators for learned index and data source for delta index
    auto dataIter = active_learned_index->begin();
    auto dataIterEnd = active_learned_index->end();

    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord deltaIter(std::numeric_limits<KeyType>::min(), prev_delta_index);
    deltaIter.advance_to_next_valid(); //required to move pos from -1 to 0 after initialization

    // Construct a radix spline builder
    rs::BuilderWithoutMinMax<KeyType> rsbuilder{};

    KeyType dataKey;
    KeyType deltaKey;

    // Compact changes with the data and train the new spline in a single pass

    // has_next = has more things for you to read
    while(deltaIter.has_next && dataIter != dataIterEnd){
    // While both indexes have elements left:
        dataKey = (*dataIter).first;    // key of current element in learned index
        deltaKey = deltaIter.get_key(); // key of current element in delta index

        // Invariant: all changes related to keys < deltaKey have been applied
        
        // if dataKey < deltaKey then add dataKey to the new merged index 
        if(dataKey < deltaKey){
            kv_new_data->push_back(*dataIter);
            rsbuilder.AddKey(dataKey); 
            dataIter++;
        }
        // if dataKey >= deltaKey then there are changes in the delta index that we have to take into account
        else{
            if(!deltaIter.get_is_removed()){    // skip deleted records
                kv_new_data->push_back(make_pair(deltaKey, deltaIter.get_val()));
                rsbuilder.AddKey(deltaKey);
            }
            deltaIter.advance_to_next_valid(); 
            if(dataKey == deltaKey) dataIter++; // Assumption: no duplicates - in case of "=", skip this record since it overwritten by the changes in the delta index
        }
    }

    // If only the learned index has elements left, just add them all in the new merged index
    while(dataIter != dataIterEnd){
        kv_new_data->push_back(*dataIter);
        rsbuilder.AddKey(dataKey);
        dataIter++;    
    }

    // If only the delta index has elements left, just add them all in the new merged index
    while(deltaIter.has_next){
        if(!deltaIter.get_is_removed()){
            kv_new_data->push_back(make_pair(deltaIter.get_key(), deltaIter.get_val()));
            rsbuilder.AddKey(deltaKey);
        }
        deltaIter.advance_to_next_valid();
    }

    next_learned_index = new LearnedIndex<KeyType, ValueType>(*kv_new_data, rsbuilder);

    // Do not change order of the following critical sections
    // Grab the mutex so that no other thread reads the indexes locations while you change them.
    // TODO: think if it would be better to merge them
    learned_index_mutex.lock();
    LearnedIndex<KeyType, ValueType> * learned_index_to_garbage_collect = active_delta_index;
    active_delta_index = next_learned_index;
    learned_index_mutex.unlock();  

    delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * delta_index_to_garbage_collect = prev_delta_index;
    prev_delta_index = nullptr;
    delta_index_mutex.unlock(); 

    next_learned_index = nullptr; // Reset next_learned_index pointer

    // TODO: think about letting each reader deciding if he has to delete the index he read in case he is the last one
    // busy wait - TODO: maybe replace with conditional variable
    while(learned_index_to_garbage_collect->readers_in < learned_index_to_garbage_collect->readers_out){}
    delete learned_index_to_garbage_collect; 

    // busy wait - TODO: maybe replace with conditional variable
    while(delta_index_to_garbage_collect->readers_in < delta_index_to_garbage_collect->readers_out){}
    delete delta_index_to_garbage_collect;

    // Unlock mutex so that more compactions can take place
    compaction_mutex.unlock();
}

#endif

// TODO: keep track of max for RadixSpline building
// TODO: write destructors for the classes
// TODO: think about different lock for delta-write and different for delta-read (compaction must take both)
// TODO: think about different lock for active-delta and different for prev-delta (compaction must take both)
// NOTE: maybe disable assertions for experiments
// NOTE: use inline functions