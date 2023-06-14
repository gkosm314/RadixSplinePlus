#ifndef BIDATASOURCE_HEADER
#define BIDATASOURCE_HEADER

#include "deltaindex.h"

template <class KeyType, class ValueType>
class BiDataSource : public Source<KeyType, ValueType>{
 public:
    BiDataSource(const KeyType begin,
                 DeltaIndex<KeyType, ValueType> * const current_delta_index, DeltaIndex<KeyType, ValueType> * const frozen_delta_index);
    void advance_to_next_valid() override;
    const KeyType &get_key() override;
    const ValueType &get_val() override;
    const bool &get_is_removed() override;
    const bool get_has_next() override;
 
 private:
    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord currentSource;
    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord frozenSource;
    typename DeltaIndex<KeyType, ValueType>::DeltaIndexRecord * answerSource;

    void advance();
    void set_answer();
};

template <class KeyType, class ValueType>
BiDataSource<KeyType,ValueType>::BiDataSource(const KeyType begin, 
                                              DeltaIndex<KeyType, ValueType> * const current_delta_index,
                                              DeltaIndex<KeyType, ValueType> * const frozen_delta_index)
                                              : currentSource(current_delta_index->get_iter(begin)),
                                                frozenSource(frozen_delta_index->get_iter(begin)) {

    // Initialize both sources
    currentSource.advance_to_next_valid();  //required to move pos from -1 to 0 after initialization
    frozenSource.advance_to_next_valid();   //required to move pos from -1 to 0 after initialization

    set_answer();
}

template <class KeyType, class ValueType>
void BiDataSource<KeyType,ValueType>::advance_to_next_valid(){
    advance();
    set_answer();
}

template <class KeyType, class ValueType>
const KeyType & BiDataSource<KeyType,ValueType>::get_key(){
    return answerSource->get_key();
}

template <class KeyType, class ValueType>
const ValueType & BiDataSource<KeyType,ValueType>::get_val(){
    return answerSource->get_val();
}

template <class KeyType, class ValueType>
const bool & BiDataSource<KeyType,ValueType>::get_is_removed(){
    return answerSource->get_is_removed();
}

template <class KeyType, class ValueType>
const bool BiDataSource<KeyType,ValueType>::get_has_next(){
    return currentSource.get_has_next() || frozenSource.get_has_next();
}

template <class KeyType, class ValueType>
void BiDataSource<KeyType,ValueType>::advance(){
    KeyType currentKey = currentSource.get_key();
    KeyType frozenKey = frozenSource.get_key();

    // If both delta's are viewable
    if(currentSource.get_has_next() && frozenSource.get_has_next()){
        // Advance the lowest key. In case of "==", advance both, since the frozen change was overwritten
        if(currentKey < frozenKey) currentSource.advance_to_next_valid();
        else if (currentKey == frozenKey){
            currentSource.advance_to_next_valid();
            frozenSource.advance_to_next_valid();
        }
        else frozenSource.advance_to_next_valid();
    }
    // If only one delta is still viewable, advance just this one
    else if(currentSource.get_has_next() && !frozenSource.get_has_next()) currentSource.advance_to_next_valid();
    else if(!currentSource.get_has_next() && frozenSource.get_has_next()) frozenSource.advance_to_next_valid();
}

template <class KeyType, class ValueType>
void BiDataSource<KeyType,ValueType>::set_answer(){
    // If both delta's are viewable
    if(currentSource.get_has_next() && frozenSource.get_has_next()){
        // Read the change concering the lowest key. In case of "==", current delta overwrites the frozen delta 
        if(currentSource.get_key() <= frozenSource.get_key()) answerSource = &currentSource;
        else answerSource = &frozenSource;       
    }
    // If only one delta is viewable, return the viewable delta directly
    else if(currentSource.get_has_next() && !frozenSource.get_has_next()) answerSource = &currentSource;
    else if(!currentSource.get_has_next() && frozenSource.get_has_next()) answerSource = &frozenSource;
}

#endif