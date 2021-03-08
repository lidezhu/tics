#pragma once

#include <algorithm>


template <class RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
    ::std::nth_element(first, nth, last);
}

template <class RandomIt>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
{
    ::std::partial_sort(first, middle, last);
}

template <class RandomIt, class Compare>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
{
    ::std::partial_sort(first, middle, last, compare);
}
