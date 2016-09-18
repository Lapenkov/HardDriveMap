#include "container.h"
#include <iostream>
#include <boost/interprocess/containers/string.hpp>

int main()
{
    using namespace boost::interprocess;

    using CharAllocator = allocator<char, managed_mapped_file::segment_manager>;
    using string = basic_string<char, std::char_traits<char>, CharAllocator>;

    HardDriveContainers::Map<string, string> a("store.tmp");
    HardDriveContainers::Map<int, int> b("storeb.tmp");

    a.Insert("value", "test");
    b.Insert(1, 2);

    return 0;
}
