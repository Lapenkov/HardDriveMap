#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/functional/hash.hpp>
#include <string>
#include <functional>

namespace HardDriveContainers
{

namespace bipc = boost::interprocess;

template <class Key,
         class Value,
         class KeyAllocator = bipc::allocator<Key, bipc::managed_mapped_file::segment_manager>,
         class ValueAllocator = bipc::allocator<Value, bipc::managed_mapped_file::segment_manager>,
         class KeyHash = boost::hash<Key>>
class Map
{
    using ValuePtr = typename ValueAllocator::pointer;
    using KeyPtr = typename KeyAllocator::pointer;

public:
    using key_type = Key;
    using value_type = Value;

    Map(const char* filename)
        : m_MappedFile(boost::interprocess::open_or_create, filename, 1024 * 1024 * 1024)
        , m_KeyAllocator(m_MappedFile.get_segment_manager())
        , m_ValueAllocator(m_MappedFile.get_segment_manager())
        , m_KeyNodeAllocator(m_MappedFile.get_segment_manager())
        , m_ValueNodeAllocator(m_MappedFile.get_segment_manager())
    {
        m_KeyNodePtrArray = m_MappedFile.find_or_construct<KeyNodePtr>("KeyNodePtrArray")[BUCKET_COUNT](nullptr);
    }

    template <typename ProvidedKeyT, typename ProvidedValueT>
    void Insert(const ProvidedKeyT& key, const ProvidedValueT& value)
    {
        InsertImpl(ConstructParam<Key, ProvidedKeyT>(key), ConstructParam<Value, ProvidedValueT>(value));
    }

    template <typename ProvidedKeyT>
    ValuePtr Find(const ProvidedKeyT& key)
    {
        return FindImpl(ConstructParam<Key, ProvidedKeyT>(key));
    }

    template <typename ProvidedKeyT>
    size_t Erase(const ProvidedKeyT& key)
    {
        return EraseImpl(ConstructParam<Key, ProvidedKeyT>(key));
    }

    template <typename ProvidedKeyT>
    size_t Count(const ProvidedKeyT& key)
    {
        return CountImpl(ConstructParam<Key, ProvidedKeyT>(key));
    }

    bipc::managed_mapped_file::segment_manager* GetSegmentManager() const
    {
        return m_MappedFile.get_segment_manager();
    }

    ~Map()
    {
    }
 
private:
    void InsertImpl(Key&& key, Value&& value)
    {
        std::pair<KeyNodePtr*, bool> result = FindKeyNode(key);
        bool foundKey = result.second;
        KeyNodePtr* keyNode = result.first;
        
        if (!foundKey)
        {
            KeyNodePtr newKeyNode = m_KeyNodeAllocator.allocate_one();
            m_KeyNodeAllocator.construct(newKeyNode, std::move(KeyNodeT()));
            newKeyNode->storedKey = m_KeyAllocator.allocate_one();
            m_KeyAllocator.construct(newKeyNode->storedKey, std::move(key));
            *keyNode = newKeyNode;
        }

        ValuePtr newValue = m_ValueAllocator.allocate_one();
        m_ValueAllocator.construct(newValue, std::move(value));

        ValueNodePtr newValueNode = m_ValueNodeAllocator.allocate_one();
        m_ValueNodeAllocator.construct(newValueNode, std::move(ValueNodeT()));
        newValueNode->storedValue = newValue;
        
        if ((*keyNode)->valueNode == nullptr)
        {
            (*keyNode)->valueNode = newValueNode;
        }
        else
        {
            ValueNodePtr* valueNode = &(*keyNode)->valueNode;

            for(; *valueNode; valueNode = &((*valueNode)->nextValueNode));
            *valueNode = newValueNode;
        }

        ++(*keyNode)->childCount;
    }

    ValuePtr FindImpl(Key&& key)
    {
        std::pair<KeyNodePtr*, bool> result = FindKeyNode(key);
        bool foundKey = result.second;
        KeyNodePtr* keyNode = result.first;

        return (foundKey) ? (*keyNode)->valueNode->storedValue : nullptr;
    }

    size_t EraseImpl(Key&& key)
    {
        const size_t keyHash = m_KeyHasher(key) % BUCKET_COUNT;
        
        KeyNodePtr keyNode = m_KeyNodePtrArray[keyHash];
        size_t destroyedValues = false;

        if (keyNode != nullptr)
        {
            // The first node contains the key to be deleted
            KeyNodePtr previous = nullptr;

            for (; keyNode; keyNode = keyNode->nextKeyNode, previous = keyNode)
            {
                if (*keyNode->storedKey == key)
                {
                    KeyNodePtr toBeDestroyed = keyNode;

                    if (!previous)
                    {
                        m_KeyNodePtrArray[keyHash] = keyNode->nextKeyNode;
                    }
                    else
                    {
                        previous->nextKeyNode = keyNode->nextKeyNode;
                    }
                    
                    destroyedValues = toBeDestroyed->childCount;

                    m_KeyNodeAllocator.destroy(toBeDestroyed);
                    m_KeyNodeAllocator.deallocate_one(toBeDestroyed);
                }
            }

            return destroyedValues;
        }
        else
        {
            return 0;
        }
    }

    size_t CountImpl(Key&& key)
    {
        std::pair<KeyNodePtr*, bool> result = FindKeyNode(key);
        bool foundKey = result.second;
        KeyNodePtr* keyNode = result.first;

        return (foundKey) ? (*keyNode)->childCount : 0;
    }

    struct ValueNodeT;
    using ValueNodeAllocator = typename ValueAllocator::template rebind<ValueNodeT>::other;
    using ValueNodePtr = typename ValueNodeAllocator::pointer;

    struct ValueNodeT
    {
        ValueNodeT()
        {}

        ValueNodeT(const ValueNodeT&) = delete;
        ValueNodeT& operator =(const ValueNodeT&) = delete;

        ValueNodeT(const ValueNodeT&& rhv)
            : storedValue(rhv.storedValue)
            , nextValueNode(rhv.nextValueNode)
        {}

        ValuePtr storedValue;
        ValueNodePtr nextValueNode {nullptr}; 

        ~ValueNodeT()
        {}
    };

    struct KeyNodeT;
    using KeyNodeAllocator = typename ValueAllocator::template rebind<KeyNodeT>::other;
    using KeyNodePtr = typename KeyNodeAllocator::pointer;

    struct KeyNodeT
    {
        KeyNodeT()
        {}

        KeyNodeT(const KeyNodeT&) = delete;
        KeyNodeT& operator =(const KeyNodeT&) = delete;

        KeyNodeT(const KeyNodeT&& rhv)
            : valueNode(rhv.valueNode)
            , nextKeyNode(rhv.nextKeyNode)
        {}

        KeyPtr storedKey;
        ValueNodePtr valueNode {nullptr};
        KeyNodePtr nextKeyNode {nullptr}; 
        size_t childCount {0};

        ~KeyNodeT()
        {}
    };

    std::pair<KeyNodePtr*, bool> FindKeyNode(const Key& key)
    {
        const size_t keyHash = m_KeyHasher(key) % BUCKET_COUNT;
        
        KeyNodePtr* keyNode = m_KeyNodePtrArray + keyHash;
        bool foundKey = false;

        if (*keyNode != nullptr)
        {
            for(; *keyNode; keyNode = &((*keyNode)->nextKeyNode))
            {
                if (*((*keyNode)->storedKey) == key)
                {
                    foundKey = true;
                    break;
                }
            }
        }
        return std::make_pair(keyNode, foundKey);
    }

    template <typename Result, typename T>
    Result ConstructParam(const T& value, typename std::enable_if<!std::is_class<T>::value && !std::is_pointer<T>::value && !std::is_array<T>::value>::type* = 0)
    {
        return Result(value);
    }

    template <typename Result, typename T>
    Result ConstructParam(const T& value, typename std::enable_if<std::is_class<T>::value || std::is_pointer<T>::value || std::is_array<T>::value>::type* = 0)
    {
        return Result(value, GetSegmentManager());
    }
private:
    static constexpr size_t BUCKET_COUNT {2 * size_t(1e6)};

    bipc::managed_mapped_file m_MappedFile;
    KeyAllocator m_KeyAllocator;
    ValueAllocator m_ValueAllocator;
    KeyNodeAllocator m_KeyNodeAllocator;
    ValueNodeAllocator m_ValueNodeAllocator;
    KeyHash m_KeyHasher;
    KeyNodePtr* m_KeyNodePtrArray;
};
} //HardDriveContainers
