#define BOOST_TEST_MODULE hard_drive_map_functional_test
#define BOOST_TEST_DYN_LINK
#include <boost/test/included/unit_test.hpp>
#include "container.h"

#include <boost/interprocess/containers/string.hpp>

BOOST_AUTO_TEST_SUITE(hdd_map_test_suite)

BOOST_AUTO_TEST_CASE(few_bucket_functional_testing, *boost::unit_test::timeout(10))
{
    using namespace boost::interprocess;

    using CharAllocator = allocator<char, managed_mapped_file::segment_manager>;
    using string = basic_string<char, std::char_traits<char>, CharAllocator>;

    const char* storeFileme = "store.tmp";

    std::remove(storeFileme);

    HardDriveContainers::Map<string, string> a(storeFileme, 1024 * 1024 * 1024ul, 50ul);

    BOOST_REQUIRE_EQUAL(a.Size(), 0ul);
    BOOST_REQUIRE_EQUAL(a.Empty(), true);

    constexpr size_t elementCount = (int)1e3;

    for (size_t i = 0; i < elementCount; ++i)
    {
        a.Insert(std::to_string(i).c_str(), std::to_string(i).c_str());
    }

    for (size_t i = 0; i < elementCount; ++i)
    {
        BOOST_REQUIRE_EQUAL(*a.Find(std::to_string(i).c_str()), std::to_string(i).c_str());
        BOOST_REQUIRE_EQUAL(a.Count(std::to_string(i).c_str()), 1ul);
    }

    const char* sampleKey = "1";
    const char* sampleValue = "Value";
    const char* sampleValue2 = "Value2";
    BOOST_REQUIRE_EQUAL(a.Size(), elementCount);
    BOOST_REQUIRE_EQUAL(a.Erase(sampleKey), 1ul);
    BOOST_REQUIRE_EQUAL(a.Size(), elementCount - 1);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 0ul);
    BOOST_CHECK(a.Find(sampleKey) == nullptr);

    a.Insert(sampleKey, sampleValue);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 1ul);
    BOOST_CHECK(*a.Find(sampleKey) == sampleValue);

    a.Insert(sampleKey, sampleValue2);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 2ul);
    BOOST_CHECK(*a.Find(sampleKey) == sampleValue2);

    a.Erase(sampleKey);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 0ul);
    BOOST_CHECK(a.Find(sampleKey) == nullptr);

    for (size_t i = 0; i < elementCount; ++i)
    {
        a.Insert(sampleKey, std::to_string(i).c_str());
    }

    BOOST_REQUIRE_EQUAL(a.Size(), elementCount * 2 - 1);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), elementCount);
    BOOST_CHECK(*a.Find(sampleKey) == std::to_string(elementCount - 1).c_str());

    a.Erase(sampleKey);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 0ul);

    for (size_t i = 0; i < elementCount; ++i)
    {
        a.Insert(sampleKey, sampleValue);
    }

    BOOST_REQUIRE_EQUAL(a.Erase(sampleKey, sampleValue), elementCount);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 0ul);

    for (size_t i = 0; i < elementCount; ++i)
    {
        a.Insert(sampleKey, sampleValue);
        a.Insert(sampleKey, sampleValue2);
    }
    
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), elementCount * 2);

    BOOST_REQUIRE_EQUAL(a.Erase(sampleKey, sampleValue), elementCount);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), elementCount);

    BOOST_REQUIRE_EQUAL(a.Erase(sampleKey, sampleValue2), elementCount);
    BOOST_REQUIRE_EQUAL(a.Count(sampleKey), 0ul);
    BOOST_REQUIRE_EQUAL(a.Size(), elementCount - 1);
    BOOST_REQUIRE_EQUAL(a.Empty(), false);
}

BOOST_AUTO_TEST_CASE(space_testing, *boost::unit_test::timeout(10))
{
}

BOOST_AUTO_TEST_SUITE_END()
