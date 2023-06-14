/*
 * The code is part of the XIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about XIndex, visit:
 *     https://ppopp20.sigplan.org/details/PPoPP-2020-papers/13/XIndex-A-Scalable-Learned-Index-for-Multicore-Data-Storage
 */

#include "util.h"

#if !defined(buffer_H)
#define buffer_H

template <class key_t, class val_t>
struct Source{
  virtual void advance_to_next_valid() = 0;
  virtual const key_t &get_key() = 0;
  virtual const val_t &get_val() = 0;
  virtual const bool &get_is_removed() = 0;
  virtual const bool get_has_next() = 0;
};

namespace rspindex {

const uint8_t alt_buf_fanout = 16;
const uint8_t node_capacity = alt_buf_fanout - 1;

template <class key_t, class val_t>
class AltBtreeBuffer {
  template <class key_t_, class val_t_, bool optt, size_t max_model_n>
  friend class Group;
  class Node;
  class Internal;
  class Leaf;

  typedef AtomicVal<val_t> atomic_val_t;
  typedef Node node_t;
  typedef Internal internal_t;
  typedef Leaf leaf_t;

  class Node {
   public:
    void lock();
    void unlock();
    bool is_full() { return key_n == node_capacity; };
    int find_first_larger_than(const key_t &key);
    int find_first_larger_than_or_equal_to(const key_t &key);
    void move_keys_backward(int begin, int end, int off);
    void move_keys_backward(int begin, int off);
    void copy_keys(int begin, int end, key_t *dst);
    void copy_keys(int begin, key_t *dst);

   public:
    bool is_leaf;
    volatile uint8_t locked = 0;
    uint8_t key_n;
    volatile uint64_t version = 0;
    key_t keys[node_capacity];
    Internal *volatile parent = nullptr;
  };

  class Internal : public Node {
   public:
    using Node::is_leaf;
    using Node::key_n;
    using Node::keys;
    using Node::locked;
    using Node::parent;
    using Node::version;

   public:
    Node *find_child(const key_t &key);
    void move_children_backward(int begin, int end, int off);
    void move_children_backward(int begin, int off);
    void copy_children(int begin, int end, Node **dst);
    void copy_children(int begin, Node **dst);

   public:
    Node *children[alt_buf_fanout];
  };

  class Leaf : public Node {
   public:
    using Node::is_leaf;
    using Node::key_n;
    using Node::keys;
    using Node::locked;
    using Node::parent;
    using Node::version;

   public:
    void move_vals_backward(int begin, int end, int off);
    void move_vals_backward(int begin, int off);
    void copy_vals(int begin, int end, atomic_val_t *dst);
    void copy_vals(int begin, atomic_val_t *dst);

   public:
    atomic_val_t vals[node_capacity];
    Leaf *next;
  };

  struct RefSource {
    typedef AtomicVal<val_t> atomic_val_t;

    RefSource(AltBtreeBuffer *buffer);
    void advance_to_next_valid();
    const key_t &get_key();
    atomic_val_t &get_val();

    leaf_t *next = nullptr;
    bool has_next = false;
    int pos = -1, n = 0;
    key_t keys[node_capacity];
    atomic_val_t *val_ptrs[node_capacity];
  };

 public:
  AltBtreeBuffer();
  ~AltBtreeBuffer();

  inline bool get(const key_t &key, val_t &val, bool &deleted_flag);
  inline bool update(const key_t &key, const val_t &val, bool &found_flag);
  inline void insert(const key_t &key, const val_t &val);
  inline bool remove(const key_t &key, const bool &delete_as_insert_flag, bool &found_flag);
  inline size_t scan(const key_t &key_begin, const size_t n,
                     std::vector<std::pair<key_t, val_t>> &result);
  inline void range_scan(const key_t &key_begin, const key_t &key_end,
                         std::vector<std::pair<key_t, val_t>> &result);

  inline uint32_t size();
  long long size_in_bytes() const;

  struct DataSource : public Source<key_t, val_t> {
    DataSource(key_t begin, AltBtreeBuffer *buffer);
    void advance_to_next_valid() override;
    const key_t &get_key() override;
    const val_t &get_val() override;
    const bool &get_is_removed() override;
    const bool get_has_next() override;

    leaf_t *next = nullptr;
    bool has_next = false;
    int pos = 0, n = 0;
    key_t keys[node_capacity];
    val_t vals[node_capacity];
    bool is_removed[node_capacity];
    // TODO: try replacing the three arrays with a vector of tuples to improve caching performance
  };  

 private:
  leaf_t *locate_leaf(key_t key, uint64_t &version);
  leaf_t *locate_leaf_locked(key_t key);

  void insert_leaf(const key_t &key, const val_t &val, leaf_t *target);
  bool split_n_insert_leaf(const key_t &key, const val_t &val, int slot,
                           leaf_t *target, const bool remove_after_insert_flag = false);

  inline void allocate_new_block();
  inline uint8_t *allocate_node();
  inline internal_t *allocate_internal();
  inline leaf_t *allocate_leaf();
  inline size_t available_node_index();

  node_t *root = nullptr;
  leaf_t *begin = nullptr;
  std::atomic<uint32_t> size_est;
  std::mutex alloc_mut;
  std::vector<uint8_t *> allocated_blocks;
  size_t next_node_i = 0;
  static const size_t node_n_per_block = alt_buf_fanout + 1;
  static const size_t node_size = std::max(sizeof(leaf_t), sizeof(internal_t));
};

}

#endif
