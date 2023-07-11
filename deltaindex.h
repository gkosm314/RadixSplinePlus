#ifndef DELTA_INDEX_HEADER
#define DELTA_INDEX_HEADER

#include <atomic>

#include "include/delta_index/helper.h"
#include "include/delta_index/buffer_impl.h"

template <class KeyType, class ValueType>
class DeltaIndex{  
 public:
    using DeltaIndexRecord = typename rspindex::AltBtreeBuffer<KeyType, KeyType>::DataSource;

    DeltaIndex();
    ~DeltaIndex();
    inline bool find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag) const; // get value of "==" key and delete status of key
    inline void insert(const KeyType &lookup_key, const ValueType &val) const;
    inline bool update(const KeyType &lookup_key, const ValueType &val, bool &found_flag) const;
    inline bool remove_in_place(const KeyType &lookup_key, bool &found_flag) const;
    inline void remove_as_insert(const KeyType &lookup_key) const;
    
    inline std::size_t length();
    inline long long memory_consumption();

    DeltaIndexRecord get_iter(const KeyType &target){
        return DeltaIndexRecord(target, buffer);
    }

    uint64_t readers_in;
    std::atomic<uint64_t> readers_out;

 private:
    rspindex::AltBtreeBuffer<KeyType, ValueType> * buffer;      
};

template <class KeyType, class ValueType>
DeltaIndex<KeyType, ValueType>::DeltaIndex() {
    buffer = new rspindex::AltBtreeBuffer<KeyType, ValueType>();

    //Initialize readers' and writers' counters
    readers_in = 0;
    readers_out = 0;
}

template <class KeyType, class ValueType>
DeltaIndex<KeyType, ValueType>::~DeltaIndex() {
    assert(readers_in == readers_out);
    delete buffer;
}

template <class KeyType, class ValueType>
inline std::size_t DeltaIndex<KeyType, ValueType>::length(){
    //Returns number of entries (including "delete" entries)
    return buffer->size();
}

template <class KeyType, class ValueType>
inline bool DeltaIndex<KeyType, ValueType>::find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag) const{
    // Finds the exact key in the delta index, if it exists.
    // Returns True if it found a record, whether it was deleted or not, otherwise it returns False
    // Attention: If the key does not exist, then &val and &deleted_flag are not changed.
    return buffer->get(lookup_key, val, deleted_flag);
}

template <class KeyType, class ValueType>
inline void DeltaIndex<KeyType, ValueType>::insert(const KeyType &lookup_key, const ValueType &val) const{
    // Insert given key-value pair in the delta index.
    // Even though we pass by reference, internally the value is inserted in the buffer, not the ref
    buffer->insert(lookup_key, val); 
}

template <class KeyType, class ValueType>
inline bool DeltaIndex<KeyType, ValueType>::update(const KeyType &lookup_key, const ValueType &val, bool &found_flag) const{
    // Update given key-value pair in the delta index.
    // Even though we pass by reference, internally the value is inserted in the buffer, not the ref
    // Returns: true if the update was successful
    //          false if the value could not be found or if a deleted value was found
    return buffer->update(lookup_key, val, found_flag); 
}

template <class KeyType, class ValueType>
inline bool DeltaIndex<KeyType, ValueType>::remove_in_place(const KeyType &lookup_key, bool &found_flag) const{
    // If the given key exists, delete it from the delta index. Otherwise return false
    return buffer->remove(lookup_key, false, found_flag);
}

template <class KeyType, class ValueType>
inline void DeltaIndex<KeyType, ValueType>::remove_as_insert(const KeyType &lookup_key) const{
    // Delete given key from delta index.
    // If the key exists, it is marked as deleted, otherwise we insert it and then mark it as deleted.
    bool found_flag;
    buffer->remove(lookup_key, true, found_flag);
}

template <class KeyType, class ValueType>
inline long long DeltaIndex<KeyType, ValueType>::memory_consumption() {
    long long res = sizeof(*this);
    if(buffer) res += buffer->size_in_bytes();
    return res;
}

#endif