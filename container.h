#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <string>
#include <functional>

namespace HardDriveContainers
{
namespace bipc = boost::interprocess;

template <class Key,
         class Value,
         class ValueAllocator = bipc::allocator<Value, bipc::managed_mapped_file::segment_manager>,
         class KeyHash = std::hash<Key>>
class Map
{
    using ValuePtr = typename ValueAllocator::pointer;

public:
    using key_type = Key;
    using value_type = Value;

    Map(const char* filename)
        : m_MappedFile(boost::interprocess::open_or_create, filename, 1024 * 1024 * 1024)
        , m_ValueAllocator(m_MappedFile.get_segment_manager())
        , m_NodeAllocator(m_MappedFile.get_segment_manager())
    {
        m_BaseNode = m_MappedFile.find_or_construct<NodePtr>("BaseNodePtr")[BUCKET_COUNT](nullptr);
    }

    void Insert(const Key& key, const Value value)
    {
        const size_t keyHash = m_KeyHasher(key) % BUCKET_COUNT;
        
        NodePtr newNode = m_NodeAllocator.allocate_one();
        m_NodeAllocator.construct(newNode, std::move(NodeT()));
        
        NodePtr* placeInto = m_BaseNode + keyHash;
        if (*placeInto == nullptr)
        {
            *placeInto = newNode;
        }
        else
        {
            for(; (*placeInto)->nextNode; placeInto = &((*placeInto)->nextNode));
            *placeInto = newNode;
        }

        ValuePtr newValue = m_ValueAllocator.allocate_one();
        m_ValueAllocator.construct(newValue, std::move(value));

        newNode->storedValue = newValue;
    }

    ValuePtr Find(const Key& key)
    {
        const size_t keyHash = m_KeyHasher(key) % BUCKET_COUNT;
        NodePtr bucket = m_BaseNode[keyHash];

        if (bucket == nullptr)
        {
            return nullptr;
        }
        else
        {
            return bucket->storedValue;
        }
    }

    size_t Erase(const Key& key)
    {
        const size_t keyHash = m_KeyHasher(key) % BUCKET_COUNT;

        return 0;
    }

    ~Map()
    {
    }
 
private:
    struct NodeT;
    using NodeAllocator = typename ValueAllocator::template rebind<NodeT>::other;
    using NodePtr = typename NodeAllocator::pointer;

    struct NodeT
    {
        NodeT()
        {}

        NodeT(const NodeT&) = delete;
        NodeT& operator =(const NodeT&) = delete;

        NodeT(const NodeT&& rhv)
            : storedValue(rhv.storedValue)
            , nextNode(rhv.nextNode)
        {}

        bool IsHead() const { return !nextNode; }

        ValuePtr storedValue;
        NodePtr nextNode {nullptr}; 

        ~NodeT()
        {}
    };

private:
    static constexpr size_t BUCKET_COUNT {2 * size_t(1e6)};

    bipc::managed_mapped_file m_MappedFile;
    ValueAllocator m_ValueAllocator;
    NodeAllocator m_NodeAllocator;
    KeyHash m_KeyHasher;
    NodePtr* m_BaseNode;
};
} //HardDriveContainers
