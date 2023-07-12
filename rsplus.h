#ifndef RSPLUS_HEADER
#define RSPLUS_HEADER

#include <vector>
#include <utility>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#include "learnedindex.h"
#include "deltaindex.h"
#include "bidatasource.h"

template <class KeyType, class ValueType>
class RSPlus{

 public:
    RSPlus(size_t num_radix_bits = 18, size_t max_error = 32, int thread_num = 1);                  
    RSPlus(std::vector<std::pair<KeyType, ValueType>> * k, size_t num_radix_bits = 18, size_t max_error = 32, int thread_num = 1);
    RSPlus(std::pair<KeyType, ValueType> * k, size_t num, size_t num_radix_bits = 18, size_t max_error = 32, int thread_num = 1);
    ~RSPlus();

    bool find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag, int thread_id = 0);
    inline void insert(const KeyType &lookup_key, const ValueType &val, int thread_id = 0);
    inline bool update(const KeyType &lookup_key, const ValueType &val, int thread_id = 0);
    inline bool remove(const KeyType &lookup_key, int thread_id = 0);    
    void compact();
    // size_t scan(const KeyType &lookup_key, const size_t num, std::pair<KeyType, ValueType> * result);

    inline std::size_t size_of_buffer();
    inline long long memory_consumption(int thread_id = 0);
 
 private:   
    void init(size_t num_radix_bits, size_t max_error, int thread_num);

    LearnedIndex<KeyType, ValueType> * active_learned_index;//L earnedIndex to which reads are directed
    LearnedIndex<KeyType, ValueType> * next_learned_index;// LearnedIndex which is currently trained
    
    DeltaIndex<KeyType, ValueType> * active_delta_index; // DeltaIndex to which writes are directed
    DeltaIndex<KeyType, ValueType> * prev_delta_index; // read-only DeltaIndex from which we read values that are currently being flashed

    int number_of_threads;
    std::mutex * writers_delta_index_mutex;   // mutex that protects active_delta_index acquired by writes from being changed by compaction
    std::mutex * readers_delta_index_mutex;   // mutex that protects active_delta_index acquired by reads from being changed by compaction

    std::atomic_flag compaction_happening = ATOMIC_FLAG_INIT; // flag that ensures that only one compaction is happening at any given time

    size_t learned_index_radix_bits;
    size_t learned_index_max_error;

    int buffer_threshold = 10; // limit after which a compaction is triggered
    //i.e. for buffer_threshold = 10, trigger a compaction when the size of the buffer is larger than the size of the learned index / 10

    bool find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag,
                            DeltaIndex<KeyType, ValueType> * const current_delta_index,
                            DeltaIndex<KeyType, ValueType> * const frozen_delta_index);
    bool find_learned_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag,
                            LearnedIndex<KeyType, ValueType> * const current_learned_index);
    // size_t scan_aux(const KeyType &lookup_key, const size_t num, std::pair<KeyType, ValueType> * result,
    //             LearnedIndex<KeyType, ValueType> * const learned_index,
    //             DeltaIndex<KeyType, ValueType> * const current_delta_index,       
    //             DeltaIndex<KeyType, ValueType> * const frozen_delta_index);    
};

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::init(size_t num_radix_bits, size_t max_error, int thread_num){

    // Initialize writer mutexes
    number_of_threads = thread_num;
    writers_delta_index_mutex = new std::mutex[number_of_threads];
    readers_delta_index_mutex = new std::mutex[number_of_threads];

    // Initialize spline parameters
    learned_index_radix_bits = num_radix_bits;
    learned_index_max_error = max_error;
    next_learned_index = nullptr;

    // Create a new empty delta index to keep changes
    active_delta_index = new DeltaIndex<KeyType, ValueType>();
    prev_delta_index = nullptr;  
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(size_t num_radix_bits, size_t max_error, int thread_num) {

    // Initialize delta indexes, spline parameters and mutexes
    init(num_radix_bits, max_error, thread_num);

    // Create empty data vector
    std::vector<std::pair<KeyType, ValueType>> * k = new std::vector<std::pair<KeyType, ValueType>>;

    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k, num_radix_bits, max_error);
 
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::vector<std::pair<KeyType, ValueType>> * k, size_t num_radix_bits, size_t max_error, int thread_num){

    // Initialize delta indexes, spline parameters and mutexes
    init(num_radix_bits, max_error, thread_num);

    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k, num_radix_bits, max_error); 
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::RSPlus(std::pair<KeyType, ValueType> * k, size_t num, size_t num_radix_bits, size_t max_error, int thread_num){

    // Initialize delta indexes, spline parameters and mutexes
    init(num_radix_bits, max_error, thread_num);

    // Initialize new learned index
    active_learned_index = new LearnedIndex<KeyType, ValueType>(k, num, num_radix_bits, max_error);  
}

template <class KeyType, class ValueType>
RSPlus<KeyType, ValueType>::~RSPlus() {
    delete active_learned_index;
    delete active_delta_index;

    // If the class is used as intended, these two should not be called
    if (prev_delta_index) delete prev_delta_index;
    if (next_learned_index) delete next_learned_index;

    delete [] writers_delta_index_mutex;
    delete [] readers_delta_index_mutex;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag, int thread_id){
    // Finds the exact key in the delta index, if it exists. Returns true if the key exists, false otherwise.
    // If the function returns false, then the value of &val is undefined

    // Pointers to indexes will not change as long as the mutex is held. We use a different mutex for each thread to avoid unnecessary congestion.
    readers_delta_index_mutex[thread_id].lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;
    LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;  

    // Initially we have not found the key
    bool key_found = false;

    // We first look in the delta index
    key_found = find_delta_index(lookup_key, val, deleted_flag, current_delta_index, frozen_delta_index);

    // If no key could be found in the deltas, 
    if(!key_found) key_found = find_learned_index(lookup_key, val, deleted_flag, current_learned_index);

    readers_delta_index_mutex[thread_id].unlock();

    return key_found && !deleted_flag;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_delta_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag,
                                                    DeltaIndex<KeyType, ValueType> * const current_delta_index,
                                                    DeltaIndex<KeyType, ValueType> * const frozen_delta_index){
    // Initially we have not found the key
    bool key_found = false;
    
    // Search for the key in the active delta index
    key_found = current_delta_index->find(lookup_key, val, deleted_flag);

    // If a frozen_delta_index is available
    if(frozen_delta_index){
        // If no key could be found in the current dela, do an additional lookup at in the previous delta
        if(!key_found) key_found = frozen_delta_index->find(lookup_key, val, deleted_flag);
    }
    
    return key_found;
}

template <class KeyType, class ValueType>
bool RSPlus<KeyType, ValueType>::find_learned_index(const KeyType &lookup_key, ValueType &val, bool &deleted_flag,
                                                    LearnedIndex<KeyType, ValueType> * const current_learned_index){
    // Initially we have not found the key
    bool key_found = false;
    deleted_flag = false;
    int temp_offset;

    key_found = current_learned_index->find(lookup_key, temp_offset, val, deleted_flag);

    return key_found;
}

template <class KeyType, class ValueType>
inline void RSPlus<KeyType, ValueType>::insert(const KeyType &lookup_key, const ValueType &val, int thread_id){
   
    // Pointers to indexes will not change as long as the mutex is held. We use a different mutex for each thread to avoid unnecessary congestion. 
    writers_delta_index_mutex[thread_id].lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    current_delta_index->insert(lookup_key, val);
    writers_delta_index_mutex[thread_id].unlock();

    // Triggers a compaction if the condition is met
    std::size_t buffer_size = size_of_buffer();
    // if(buffer_size >= 10000 && buffer_size >= active_learned_index->length()/buffer_threshold) compact();
}

template <class KeyType, class ValueType>
inline bool RSPlus<KeyType, ValueType>::update(const KeyType &lookup_key, const ValueType &val, int thread_id){

    // Assumption: we assume that the value already exists in the index
    bool update_result =  false;

    // Pointers to indexes will not change as long as the mutex is held. We use a different mutex for each thread to avoid unnecessary congestion.
    writers_delta_index_mutex[thread_id].lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;

    /*  If frozen_delta_index is a nullptr, then you got the right to change the current_delta_index before the compactions started
        since you hold a writer's mutex and as a result compact() will wait for you before compacting this delta and the associated learned index */ 
    // We will perform update-in-place.  If we cannot find a value, that means it was inserted after the update or that it does not exist.
    if(frozen_delta_index == nullptr){
        // Try to update the value in the current_delta_index
        bool key_found_in_delta = false;
        bool update_in_delta_index_result = current_delta_index->update(lookup_key, val, key_found_in_delta);
        
        // If you didn't find the key in the delta index, attempt to search for it in the learned index
        bool update_in_learned_index_result = false;  //if the key was found but it is_removed then the update will return false               
        if(!key_found_in_delta){
                LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;

                int key_position;
                bool deleted_flag = false;
                bool key_found_in_learned_index = current_learned_index->find(lookup_key, key_position, deleted_flag);

                if(key_found_in_learned_index && !deleted_flag) update_in_learned_index_result = current_learned_index->update(key_position, val);
        }

        update_result =  update_in_delta_index_result || update_in_learned_index_result;
    }
     // If frozen_delta_index is not a nullptr, then the compaction has started, so perform the update as an insert
    else {
        current_delta_index->insert(lookup_key, val);
        update_result = true;
    }

    writers_delta_index_mutex[thread_id].unlock();

    return update_result;
}

template <class KeyType, class ValueType>
inline bool RSPlus<KeyType, ValueType>::remove(const KeyType &lookup_key, int thread_id){
    
    // Assumption: we assume that the value already exists in the index
    bool remove_result =  false;

    // Pointers to indexes will not change as long as the mutex is held. We use a different mutex for each thread to avoid unnecessary congestion.
    writers_delta_index_mutex[thread_id].lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;

    /*  If frozen_delta_index is a nullptr, then you got the right to change the current_delta_index before the compactions started
        since you hold a writer's mutex and as a result compact() will wait for you before compacting this delta and the associated learned index */ 
    // We will perform remove-in-place.  If we cannot find a value, that means it was inserted after the remove or that it does not exist.
    if(frozen_delta_index == nullptr){
        // Try to remove the value in the current_delta_index
        bool key_found_in_delta = false;
        bool remove_in_delta_index_result = current_delta_index->remove_in_place(lookup_key, key_found_in_delta);
        
        // If you didn't find the key in the delta index, attempt to search for it in the learned index
        bool remove_in_learned_index_result = false;  //if the key was found but it is_removed then the remove will return false               
        if(!key_found_in_delta){
                LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;

                int key_position;
                bool deleted_flag = false;
                bool key_found_in_learned_index = current_learned_index->find(lookup_key, key_position, deleted_flag);

                if(key_found_in_learned_index && !deleted_flag) remove_in_learned_index_result = current_learned_index->remove(key_position);
        }

        remove_result =  remove_in_delta_index_result || remove_in_learned_index_result;
    }
    
    // If frozen_delta_index is not a nullptr, then the compaction has started, so perform the remove as an insert
    else {
        current_delta_index->remove_as_insert(lookup_key);
        remove_result = true;
    }

    writers_delta_index_mutex[thread_id].unlock();

    return remove_result;
}

template <class KeyType, class ValueType>
void RSPlus<KeyType, ValueType>::compact(){

    // If another compaction is taking place then abort the compaction
    if(compaction_happening.test_and_set()) return;

    // Allocate memory for the new buffer before you take the locks in order to hold the locks as little as possible
    DeltaIndex<KeyType, ValueType> * new_buffer = new DeltaIndex<KeyType, ValueType>();
    
    // Grab the mutexes in order to block read and write requests to safely change the buffer pointers.
    // Otherwise, a request may get an invalid intermediate state of the pointer. Also, we would not be able to provide the following guarantee
    for(int i = 0; i < number_of_threads; i++) readers_delta_index_mutex[i].lock();
    for(int i = 0; i < number_of_threads; i++) writers_delta_index_mutex[i].lock();
    // Guarantee: no incomplete write request for the active_delta_index when we freeze it, since there are no incomplete write or read requests because we hold the mutexes

    // Change the pointers so that the "previous" delta is the delta that was active until now and the active delta is the new, empty delta
    // Remember these assignments are not atomic
    prev_delta_index = active_delta_index;
    active_delta_index = new_buffer;

    // Allow write and read request continue
    for(int i = 0; i < number_of_threads; i++) readers_delta_index_mutex[i].unlock();
    for(int i = 0; i < number_of_threads; i++) writers_delta_index_mutex[i].unlock();
    
    // Guarantee: after this point, writes are directed to the new buffer and there is no write going on in the prev_delta_index buffer and we can start merging without waiting

    // Note: length of prev_delta_index may increase by the ongoing writes, but push_backs will resize the vector's size automatically
    std::vector<std::pair<KeyType, ValueType>> * kv_new_data = new std::vector<std::pair<KeyType, ValueType>>;
    kv_new_data->reserve(active_learned_index->length() + prev_delta_index->length()); 
    
    // Grab iterators for learned index and data source for delta index
    auto dataIter = active_learned_index->begin();
    auto dataIterEnd = active_learned_index->end();

    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord deltaIter = prev_delta_index->get_iter(std::numeric_limits<KeyType>::min());
    deltaIter.advance_to_next_valid(); //required to move pos from -1 to 0 after initialization

    // Construct a radix spline builder
    rsplus::BuilderWithoutMinMax<KeyType> rsbuilder{learned_index_radix_bits, learned_index_max_error};

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
            if(!active_learned_index->get_is_removed(dataIter)){
                kv_new_data->push_back(*dataIter);
                rsbuilder.AddKey(dataKey); 
            }
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
        if(!active_learned_index->get_is_removed(dataIter)){
            kv_new_data->push_back(*dataIter);
            rsbuilder.AddKey((*dataIter).first);
        }
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

    // Grab the mutexes in order to block read requests
    for(int i = 0; i < number_of_threads; i++) readers_delta_index_mutex[i].lock();
    // Guarantee: no incomplete read request for the active_learned_index, since there are no incomplete read requests because we hold the mutexes
    LearnedIndex<KeyType, ValueType> * learned_index_to_garbage_collect = active_learned_index;
    active_learned_index = next_learned_index;

    DeltaIndex<KeyType, ValueType> * delta_index_to_garbage_collect = prev_delta_index;
    prev_delta_index = nullptr;
    for(int i = 0; i < number_of_threads; i++) readers_delta_index_mutex[i].unlock(); 
    // Guarantee: after this point, reads are directed to the new indexes and there is no read going on in the previous indexes, so we can safely delete them

    next_learned_index = nullptr; // Reset next_learned_index pointer

    // Change the flag, since no other compaction is happening
    compaction_happening.clear();

    // busy wait
    delete learned_index_to_garbage_collect; 
    delete delta_index_to_garbage_collect;
}

// template <class KeyType, class ValueType>
// size_t RSPlus<KeyType, ValueType>::scan(const KeyType &lookup_key, const size_t num, std::pair<KeyType, ValueType> * result) {
//     // Get reference to delta indexes. Compaction cannot change them while we hold the lock.
//     readers_delta_index_mutex[thread_id].lock();
//     DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
//     DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;  
//     LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;
//     current_delta_index->readers_in++;  // atomic because we are in a shared critical section
//     if(frozen_delta_index) frozen_delta_index->readers_in++;  // atomic because we are in a shared critical section
//     readers_delta_index_mutex[thread_id].unlock();
  
//     // Call the correct version of the scan function - readers_out is increased inside each function call
//     return scan_aux(lookup_key, num, result, current_learned_index, current_delta_index, frozen_delta_index);
// }

// template <class KeyType, class ValueType>
// size_t RSPlus<KeyType, ValueType>::scan_aux(const KeyType &lookup_key, const size_t num, std::pair<KeyType, ValueType> * result,
//             LearnedIndex<KeyType, ValueType> * const learned_index,
//             DeltaIndex<KeyType, ValueType> * const current_delta_index,       
//             DeltaIndex<KeyType, ValueType> * const frozen_delta_index) {       

//     bool learned_index_readers_updated = false;     
//     bool delta_index_readers_updated = false;     

//     // Grab iterators for learned index and data source for delta index
//     int learned_index_offset;
//     // get offset using lookup(), if offset is valid then increase begin iterator by that many positions, otherwise assign end iterator
//     auto dataIter = learned_index->lookup(lookup_key,learned_index_offset) ? learned_index->begin() + learned_index_offset : learned_index->end();
//     auto dataIterEnd = learned_index->end();

//     Source<KeyType, ValueType> * deltaIter;

//     if(frozen_delta_index) {
//         BiDataSource<KeyType, ValueType> deltaIterObj(lookup_key, current_delta_index, frozen_delta_index);
//         deltaIter = &deltaIterObj;
//     }
//     else {
//         typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord deltaIterObj = current_delta_index->get_iter(lookup_key);
//         deltaIterObj.advance_to_next_valid(); //required to move pos from -1 to 0 after initialization
//         deltaIter = &deltaIterObj;
//     }

//     size_t records_scanned = 0; // how many records we have scanned so far

//     KeyType dataKey;
//     KeyType deltaKey;       

//     // has_next = has more things for you to read
//     while(records_scanned < num && deltaIter->get_has_next() && dataIter != dataIterEnd){
//     // While both indexes have elements left:
//         dataKey = (*dataIter).first;    // key of current element in learned index
//         deltaKey = deltaIter->get_key(); // key of current element in delta index

//         // Invariant: all changes related to keys < deltaKey have been applied
        
//         // if dataKey < deltaKey then add dataKey to the results
//         if(dataKey < deltaKey){
//             if(!active_learned_index->get_is_removed(dataIter)) result[records_scanned++] = (*dataIter);
//             dataIter++;
//         }
//         // if dataKey >= deltaKey then there are changes in the delta index that we have to take into account
//         else{
//             if(!deltaIter->get_is_removed()) result[records_scanned++] = std::make_pair(deltaKey, deltaIter->get_val());
//             deltaIter->advance_to_next_valid(); 
//             if(dataKey == deltaKey) dataIter++; // Assumption: no duplicates - in case of "=", skip this record since it overwritten by the changes in the delta index
//         }
//     }

//     // If only the learned index has elements left, just add as many as you can to the results
//     while(records_scanned < num && dataIter != dataIterEnd){
//         if(!active_learned_index->get_is_removed(dataIter)) result[records_scanned++] = (*dataIter);
//         dataIter++;    
//     }

//     // If only the delta index has elements left, just add as many as you can to the results
//     while(records_scanned < num && deltaIter->get_has_next()){
//         if(!deltaIter->get_is_removed()) result[records_scanned++] = std::make_pair(deltaIter->get_key(), deltaIter->get_val());
//         deltaIter->advance_to_next_valid();
//     }

//     // Increase readers_out counter
//     current_delta_index->readers_out++;  // atomic because we are out of the critical section        
//     if(frozen_delta_index) frozen_delta_index->readers_out++;  // atomic because we are out of the critical section       

//     return records_scanned;
// }

template <class KeyType, class ValueType>
inline std::size_t RSPlus<KeyType, ValueType>::size_of_buffer(){
    return active_delta_index->length();
}

template <class KeyType, class ValueType>
inline long long RSPlus<KeyType, ValueType>::memory_consumption(int thread_id){

    long long res = 0;

    // Treat locks in the same way as you treat them for find()
    readers_delta_index_mutex[thread_id].lock();
    DeltaIndex<KeyType, ValueType> * const current_delta_index = active_delta_index;
    DeltaIndex<KeyType, ValueType> * const frozen_delta_index = prev_delta_index;
    LearnedIndex<KeyType, ValueType> * const current_learned_index = active_learned_index;      

    res += current_learned_index->memory_consumption();
    res += current_delta_index->memory_consumption();
    if(frozen_delta_index) res += frozen_delta_index->memory_consumption();
    readers_delta_index_mutex[thread_id].unlock();

    return res;
}

#endif