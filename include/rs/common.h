#pragma once

#include <cstddef>
#include <cstdint>

namespace rsplus {

// A CDF coordinate.
template <class KeyType>
struct Coord {
  KeyType x;
  double y;
};

struct SearchBound {
  size_t begin;
  size_t end;  // Exclusive.
};

}  // namespace rsplus
