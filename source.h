#ifndef SOURCE_HEADER
#define SOURCE_HEADER

template <class key_t, class val_t>
struct Source{
  virtual void advance_to_next_valid() = 0;
  virtual const key_t &get_key() = 0;
  virtual const val_t &get_val() = 0;
  virtual const bool &get_is_removed() = 0;
  virtual const bool get_has_next() = 0;
};

#endif