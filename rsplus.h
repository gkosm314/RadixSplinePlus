#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>
#include <utility>
#include <iostream>
#include <mutex>

#include "learnedindex.h"
#include "deltaindex.h"
#include "bidatasource.h"

template <class KeyType, class ValueType>
class RSPlus{

 public:
    RSPlus();                  
    RSPlus(std::vector<std::pair<KeyType, ValueType>> * k);
    ~RSPlus();

    bool find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag);
    inline void insert(const KeyType &lookup_key, const ValueType &val);
    inline bool update(const KeyType &lookup_key, const ValueType &val);
    inline void remove(const KeyType &lookup_key);    
    void compact();
    size_t scan(const KeyType &lookup_key, const size_t num, std::vector<std::pair<KeyType, ValueType>> & result);
 
 private:   
    LearnedIndex<KeyType, ValueType> * active_learned_index;//L earnedIndex to which reads are directed
    LearnedIndex<KeyType, ValueType> * next_learned_index;// LearnedIndex which is currently trained
    
    DeltaIndex<KeyType, ValueType> * active_delta_index; // DeltaIndex to which writes are directed
    DeltaIndex<KeyType, ValueType> * prev_delta_index; // read-only DeltaIndex from which we read values that are currently being flashed

    std::mutex writers_delta_index_mutex;   // mutex that protects active_delta_index acquired by writes from being changed by compaction
    std::mutex readers_delta_index_mutex;   // mutex that protects active_delta_index acquired by reads from being changed by compaction
    std::mutex learned_index_mutex; // mutex that protects active_learned_index from being changed by compaction
    std::mutex compaction_mutex;    // mutex that ensures that only one compaction can take place at a given time

    bool find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag);
    bool find_learned_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag);
    size_t scan_aux(const KeyType &lookup_key, const size_t num, std::vector<std::pair<KeyType, ValueType>> & result,
                LearnedIndex<KeyType, ValueType> * const learned_index,
                DeltaIndex<KeyType, ValueType> * const current_delta_index,       
                DeltaIndex<KeyType, ValueType> * const frozen_delta_index);    
};

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus() {

    // Create empty data vector
    std::vector<std::pair<KeyType, ValueType>> * k = new std::vector<std::pair<KeyType, ValueType>>;

    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k);
    next_learned_index = nullptr;

    // Create a new empty delta index to keep changes
    active_delta_index = new DeltaIndex<KeyType, ValueType>();
    prev_delta_index = nullptr;   
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::vector<std::pair<KeyType, ValueType>> * k){
    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k);
    next_learned_index = nullptr;

    // Create a new empty delta index to keep changes
    active_delta_index = new DeltaIndex<KeyType, ValueType>();
    prev_delta_index = nullptr;   
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::~RSPlus() {
    delete active_learned_index;
    delete active_delta_index;

    // If the class is used as intended, these two should not be called
    if (prev_delta_index) delete prev_delta_index;
    if (next_learned_index) delete next_learned_index;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag){
    // Finds the exact key in the delta index, if it exists. Returns true if the key exists, false otherwise.
    // If the function returns false, then the value of &val is undefined

    // Initially we have not found the key
    bool key_found = false;

    // We first look in the delta index
    key_found = find_delta_index(lookup_key, val, deleted_flag);

    // If no key could be found in the deltas, 
    if(!key_found) key_found = find_learned_index(lookup_key, val, deleted_flag);

    return key_found && !deleted_flag;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag){
    // Initially we have not found the key
    bool key_found = false;

    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase
    readers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;  
    current_delta_index->readers_in++; 
    if(frozen_delta_index) frozen_delta_index->readers_in++;
    readers_delta_index_mutex.unlock();
    
    // Search for the key in the active delta index
    key_found = current_delta_index->find(lookup_key, val, deleted_flag);

    // If a frozen_delta_index is available
    if(frozen_delta_index){
        // If no key could be found in the current dela, do an additional lookup at in the previous delta
        if(!key_found) key_found = frozen_delta_index->find(lookup_key, val, deleted_flag);
        frozen_delta_index->readers_out++; // atomic because we are out of the critical section
    }

    current_delta_index->readers_out++; // atomic because we are out of the critical section
    
    return key_found;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_learned_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag){
    // Initially we have not found the key
    bool key_found = false;
    
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

    return key_found;
}

template <class KeyType, class ValueType>
inline void RSPlus<KeyType, ValueType>::insert(const KeyType &lookup_key, const ValueType &val){
   
    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase   
    writers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    current_delta_index->writers_in++;
    writers_delta_index_mutex.unlock();

    current_delta_index->insert(lookup_key, val);
    current_delta_index->writers_out++; // atomic because we are out of the critical section
}

template <class KeyType, class ValueType>
inline bool RSPlus<KeyType, ValueType>::update(const KeyType &lookup_key, const ValueType &val){

    // Assumption: we assume that the value already exists in the index
    bool update_result =  false;

    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase   
    writers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;
    current_delta_index->writers_in++;
    writers_delta_index_mutex.unlock();

    // If frozen_delta_index is a nullptr, then you got the right to change the current_delta_index before the compactions started
    // since you increase the writers_in counter, compact will wait for you before compacting this delta and the associated learned index  
    // We will perform update-in-place.  If we cannot find a value, that means it was inserted after the update or that it does not exist.
    if(frozen_delta_index == nullptr){
        // Try to update the value in the current_delta_index
        bool key_found_in_delta = false;
        bool update_in_delta_index_result = current_delta_index->update(lookup_key, val, key_found_in_delta);
        
        // If you didn't find the key in the delta index, attempt to search for it in the learned index
        bool update_in_learned_index_result = false;
        if(!key_found_in_delta){
                LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;

                int key_position;
                bool key_found_in_learned_index = current_learned_index->find(lookup_key, key_position);

                if(key_found_in_learned_index) update_in_learned_index_result = current_learned_index->update(key_position, val); //if the key was found, the update is always successful                
        }

        update_result =  update_in_delta_index_result || update_in_learned_index_result;
    }
     // If frozen_delta_index is not a nullptr, then the compaction has started, so perform the update as an insert
    else {
        current_delta_index->insert(lookup_key, val);
        update_result = true;
    }

    current_delta_index->writers_out++; // do not increase writers_out before insertion or before updating the learned index. Leave this here!
    return update_result;
}

template <class KeyType, class ValueType>
inline void RSPlus<KeyType, ValueType>::remove(const KeyType &lookup_key){

    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase   
    writers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    current_delta_index->writers_in++;
    writers_delta_index_mutex.unlock();

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
    readers_delta_index_mutex.lock();
    writers_delta_index_mutex.lock();
    prev_delta_index = active_delta_index;
    active_delta_index = new_buffer;
    readers_delta_index_mutex.unlock();
    writers_delta_index_mutex.unlock();
    // Guarantee: prev_delta_index->writers_in will not be increased after this point

    // allocate memory before waiting => give as much time as possible to the writers to finish => wait as little as possible
    // Note: length of prev_delta_index may increase by the ongoing writes, but push_backs will resize the vector's size automatically
    std::vector<std::pair<KeyType, ValueType>> * kv_new_data = new std::vector<std::pair<KeyType, ValueType>>;
    kv_new_data->reserve(active_learned_index->length() + prev_delta_index->length()); 

    // busy wait
    while(prev_delta_index->writers_in > prev_delta_index->writers_out){}
    assert(prev_delta_index->writers_in == prev_delta_index->writers_out);
    // We suppose that no changes happen to the prev_delta_index and to the active_learned_index after this point
    
    // Grab iterators for learned index and data source for delta index
    auto dataIter = active_learned_index->begin();
    auto dataIterEnd = active_learned_index->end();

    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord deltaIter = prev_delta_index->get_iter(std::numeric_limits<KeyType>::min());
    deltaIter.advance_to_next_valid(); //required to move pos from -1 to 0 after initialization

    // Construct a radix spline builder
    rs::BuilderWithoutMinMax<KeyType> rsbuilder{};

    KeyType dataKey;
    KeyType deltaKey;

    // Compact changes with the data and train the new spline in a single pass

    // has_next = has more things for you to read
    while(deltaIter.get_has_next() && dataIter != dataIterEnd){
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
                kv_new_data->push_back(std::make_pair(deltaKey, deltaIter.get_val()));
                rsbuilder.AddKey(deltaKey);
            }
            deltaIter.advance_to_next_valid(); 
            if(dataKey == deltaKey) dataIter++; // Assumption: no duplicates - in case of "=", skip this record since it overwritten by the changes in the delta index
        }
    }

    // If only the learned index has elements left, just add them all in the new merged index
    while(dataIter != dataIterEnd){
        kv_new_data->push_back(*dataIter);
        rsbuilder.AddKey((*dataIter).first);
        dataIter++;    
    }

    // If only the delta index has elements left, just add them all in the new merged index
    while(deltaIter.get_has_next()){
        if(!deltaIter.get_is_removed()){
            kv_new_data->push_back(std::make_pair(deltaIter.get_key(), deltaIter.get_val()));
            rsbuilder.AddKey(deltaIter.get_key());
        }
        deltaIter.advance_to_next_valid();
    }

    next_learned_index = new LearnedIndex<KeyType, ValueType>(kv_new_data, rsbuilder);

    // Do not change order of the following critical sections
    // Grab the mutex so that no other thread reads the indexes locations while you change them.
    learned_index_mutex.lock();
    LearnedIndex<KeyType, ValueType> * learned_index_to_garbage_collect = active_learned_index;
    active_learned_index = next_learned_index;
    learned_index_mutex.unlock();  

    // only readers mutex required since writers do not get a reference to prev_delta_index
    readers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * delta_index_to_garbage_collect = prev_delta_index;
    prev_delta_index = nullptr;
    readers_delta_index_mutex.unlock(); 

    next_learned_index = nullptr; // Reset next_learned_index pointer

    // busy wait
    while(learned_index_to_garbage_collect->readers_in > learned_index_to_garbage_collect->readers_out){}
    delete learned_index_to_garbage_collect; 

    // busy wait
    while(delta_index_to_garbage_collect->readers_in > delta_index_to_garbage_collect->readers_out){}
    delete delta_index_to_garbage_collect;

    // Unlock mutex so that more compactions can take place
    compaction_mutex.unlock();
}

template <class KeyType, class ValueType>
size_t RSPlus<KeyType, ValueType>::scan(const KeyType &lookup_key, const size_t num, std::vector<std::pair<KeyType, ValueType>> & result) {
    // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase
    readers_delta_index_mutex.lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;  
    current_delta_index->readers_in++;
    if(frozen_delta_index) frozen_delta_index->readers_in++;
    readers_delta_index_mutex.unlock();

    // Get reference to the learned index. Compaction cannot change them while we hold the lock.
    // mutex => no concurrent increases => no need for atomic increase
    learned_index_mutex.lock();
    LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;
    current_learned_index->readers_in++; 
    learned_index_mutex.unlock();

    // Prepare results vector
    result.clear();
    result.reserve(num);
    
    // Call the correct version of the scan function - readers_out is increased inside each function call
    return scan_aux(lookup_key, num, result, current_learned_index, current_delta_index, frozen_delta_index);
}

template <class KeyType, class ValueType>
size_t RSPlus<KeyType, ValueType>::scan_aux(const KeyType &lookup_key, const size_t num, std::vector<std::pair<KeyType, ValueType>> & result,
            LearnedIndex<KeyType, ValueType> * const learned_index,
            DeltaIndex<KeyType, ValueType> * const current_delta_index,       
            DeltaIndex<KeyType, ValueType> * const frozen_delta_index) {       

    bool learned_index_readers_updated = false;     
    bool delta_index_readers_updated = false;     

    // Grab iterators for learned index and data source for delta index
    int learned_index_offset;
    // get offset using lookup(), if offset is valid then increase begin iterator by that many positions, otherwise assign end iterator
    auto dataIter = learned_index->lookup(lookup_key,learned_index_offset) ? learned_index->begin() + learned_index_offset : learned_index->end();
    auto dataIterEnd = learned_index->end();

    Source<KeyType, ValueType> * deltaIter;

    if(frozen_delta_index) {
        BiDataSource<KeyType, ValueType> deltaIterObj(lookup_key, current_delta_index, frozen_delta_index);
        deltaIter = &deltaIterObj;
    }
    else {
        typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord deltaIterObj = current_delta_index->get_iter(lookup_key);
        deltaIterObj.advance_to_next_valid(); //required to move pos from -1 to 0 after initialization
        deltaIter = &deltaIterObj;
    }

    size_t records_left = num; // how many records are left to scan

    KeyType dataKey;
    KeyType deltaKey;       

    // has_next = has more things for you to read
    while(records_left && deltaIter->get_has_next() && dataIter != dataIterEnd){
    // While both indexes have elements left:
        dataKey = (*dataIter).first;    // key of current element in learned index
        deltaKey = deltaIter->get_key(); // key of current element in delta index

        // Invariant: all changes related to keys < deltaKey have been applied
        
        // if dataKey < deltaKey then add dataKey to the results
        if(dataKey < deltaKey){
            result.push_back(*dataIter);
            records_left--;
            dataIter++;
        }
        // if dataKey >= deltaKey then there are changes in the delta index that we have to take into account
        else{
            if(!deltaIter->get_is_removed()){    // skip deleted records
                result.push_back(std::make_pair(deltaKey, deltaIter->get_val()));
                records_left--;
            }
            deltaIter->advance_to_next_valid(); 
            if(dataKey == deltaKey) dataIter++; // Assumption: no duplicates - in case of "=", skip this record since it overwritten by the changes in the delta index
        }
    }

    // For the indexes that we are not going to read again, increase the readers_out counter
    if(dataIter == dataIterEnd) {
        learned_index_readers_updated = true;
        learned_index->readers_out++;  // atomic because we are out of the critical section
    }
    if(!deltaIter->get_has_next()){
        delta_index_readers_updated = true;
        current_delta_index->readers_out++;  // atomic because we are out of the critical section        
        if(frozen_delta_index) frozen_delta_index->readers_out++;  // atomic because we are out of the critical section        
    }

    // If only the learned index has elements left, just add as many as you can to the results
    while(records_left && dataIter != dataIterEnd){
        result.push_back(*dataIter);
        records_left--;
        dataIter++;    
    }

    // If only the delta index has elements left, just add as many as you can to the results
    while(records_left && deltaIter->get_has_next()){
        if(!deltaIter->get_is_removed()){
            result.push_back(std::make_pair(deltaIter->get_key(), deltaIter->get_val()));
            records_left--;
        }
        deltaIter->advance_to_next_valid();
    }

    // For the indexes that whose readers_out counter was not increased, increase the counter
    if(!learned_index_readers_updated) learned_index->readers_out++;  // atomic because we are out of the critical section
    if(!delta_index_readers_updated) {
        current_delta_index->readers_out++;  // atomic because we are out of the critical section        
        if(frozen_delta_index) frozen_delta_index->readers_out++;  // atomic because we are out of the critical section       
    } 

    return num - records_left;
}

#endif