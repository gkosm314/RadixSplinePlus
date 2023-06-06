#ifndef DELTA_INDEX_HEADER
#define DELTA_INDEX_HEADER

#include <atomic>

#include "include/delta_index/helper.h"
#include "include/delta_index/xindex_buffer_impl.h"

template <class KeyType, class ValueType>
class DeltaIndex{  
 public:
    using DeltaIndexRecord = typename xindex::AltBtreeBuffer<KeyType, KeyType>::DataSource;

    DeltaIndex();
    ~DeltaIndex();
    inline bool find(const KeyType &lookup_key, ValueType &val, bool &deleted_flag) const; // get value of "==" key and delete status of key
    inline void insert(const KeyType &lookup_key, const ValueType &val) const;
    inline void remove(const KeyType &lookup_key) const;
    inline std::size_t length();

    DeltaIndexRecord get_iter(const KeyType &target){
        return DeltaIndexRecord(target, buffer);
    }

    uint64_t readers_in;
    std::atomic<uint64_t> readers_out;
    
    uint64_t writers_in;
    std::atomic<uint64_t> writers_out;  

 private:
    xindex::AltBtreeBuffer<KeyType, ValueType> * buffer;      
};

template <class KeyType, class ValueType>
DeltaIndex<KeyType, ValueType>::DeltaIndex() {
    buffer = new xindex::AltBtreeBuffer<KeyType, ValueType>();

    //Initialize readers' and writers' counters
    readers_in = 0;
    readers_out = 0;
    writers_in = 0;
    writers_out = 0;
}

template <class KeyType, class ValueType>
DeltaIndex<KeyType, ValueType>::~DeltaIndex() {
    assert(writers_in == writers_out);
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
    // Delete given key-value pair in the delta index.
    // Even though we pass by reference, internally the value is inserted in the buffer, not the ref
    buffer->insert(lookup_key, val); 
}

template <class KeyType, class ValueType>
inline void DeltaIndex<KeyType, ValueType>::remove(const KeyType &lookup_key) const{
    // Delete given key from delta index.
    // If the key exists, it is marked as deleted, otherwise we insert it and then mark it as deleted.
    buffer->remove(lookup_key);
}

#endif