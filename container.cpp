#include "container.h"
#include <iostream>

int main()
{
    HardDriveContainers::Map<std::string, std::string> a("store.tmp");

    a.Insert("key", "value");

    std::cout << *a.Find("key") << std::endl;

    return 0;
}
