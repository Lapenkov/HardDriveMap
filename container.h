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

    Map(const char* filename, const size_t fileSize = DEFAULT_FILE_SISE, const size_t bucketCount = DEFAULT_BUCKET_COUNT)
        : m_BucketCount(bucketCount)
        , m_MappedFile(boost::interprocess::open_or_create, filename, fileSize)
        , m_KeyAllocator(m_MappedFile.get_segment_manager())
        , m_ValueAllocator(m_MappedFile.get_segment_manager())
        , m_KeyNodeAllocator(m_MappedFile.get_segment_manager())
        , m_ValueNodeAllocator(m_MappedFile.get_segment_manager())
    {
        m_KeyNodePtrArray = m_MappedFile.find_or_construct<KeyNodePtr>("KeyNodePtrArray")[m_BucketCount](nullptr);
        m_Size = m_MappedFile.find_or_construct<size_t>("Size")(0);
    }

    Map(const Map&) = delete;
    Map& operator =(const Map&) = delete;

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

    template <typename ProvidedKeyT, typename ProvidedValueT>
    size_t Erase(const ProvidedKeyT& key, const ProvidedValueT& value)
    {
        return EraseImpl(ConstructParam<Key, ProvidedKeyT>(key), ConstructParam<Value, ProvidedValueT>(value));
    }

    template <typename ProvidedKeyT>
    size_t Count(const ProvidedKeyT& key)
    {
        return CountImpl(ConstructParam<Key, ProvidedKeyT>(key));
    }

    size_t Size() const
    {
        return *m_Size;
    }

    bool Empty() const
    {
        return *m_Size == 0;
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
        
        newValueNode->nextValueNode = (*keyNode)->valueNode;
        (*keyNode)->valueNode = newValueNode;
        
        ++(*keyNode)->childCount;
        ++(*m_Size);
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
        const size_t keyHash = m_KeyHasher(key) % m_BucketCount;
        
        KeyNodePtr keyNode = m_KeyNodePtrArray[keyHash];
        size_t erasedValues = 0ul;

        if (keyNode != nullptr)
        {
            KeyNodePtr previous = nullptr;

            for (; keyNode; previous = keyNode, keyNode = keyNode->nextKeyNode)
            {
                if (*keyNode->storedKey == key)
                {
                    ValueNodePtr valueNode = keyNode->valueNode;

                    for (; valueNode;)
                    {
                        ValueNodePtr toBeDestroyed = valueNode;

                        valueNode = valueNode->nextValueNode;

                        m_ValueNodeAllocator.destroy(toBeDestroyed);
                        m_ValueNodeAllocator.deallocate_one(toBeDestroyed);
                    }

                    KeyNodePtr toBeDestroyed = keyNode;

                    if (!previous)
                    {
                        m_KeyNodePtrArray[keyHash] = keyNode->nextKeyNode;
                    }
                    else
                    {
                        previous->nextKeyNode = keyNode->nextKeyNode;
                    }
                    
                    erasedValues = toBeDestroyed->childCount;

                    m_KeyNodeAllocator.destroy(toBeDestroyed);
                    m_KeyNodeAllocator.deallocate_one(toBeDestroyed);
                }
            }

            (*m_Size) -= erasedValues;
            return erasedValues;
        }
        else
        {
            return 0;
        }
    }

    size_t EraseImpl(Key&& key, Value&& value)
    {
        const size_t keyHash = m_KeyHasher(key) % m_BucketCount;
        
        KeyNodePtr keyNode = m_KeyNodePtrArray[keyHash];
        size_t erasedValues = 0ul;

        if (keyNode != nullptr)
        {
            KeyNodePtr previousKeyNode = nullptr;

            for (; keyNode; previousKeyNode = keyNode, keyNode = keyNode->nextKeyNode)
            {
                if (*keyNode->storedKey == key)
                {
                    ValueNodePtr valueNode = keyNode->valueNode, previousValueNode = nullptr;
                    for (; valueNode;)
                    {
                        if (*valueNode->storedValue == value)
                        {
                            ValueNodePtr toBeDestroyed = valueNode;

                            if (!previousValueNode)
                            {
                                keyNode->valueNode = valueNode->nextValueNode;
                                valueNode = keyNode->valueNode;
                            }
                            else
                            {
                                previousValueNode->nextValueNode = valueNode->nextValueNode;
                                valueNode = previousValueNode->nextValueNode;
                            }

                            m_ValueNodeAllocator.destroy(toBeDestroyed);
                            m_ValueNodeAllocator.deallocate_one(toBeDestroyed);
                            ++erasedValues;
                        }
                        else
                        {
                            previousValueNode = valueNode;
                            valueNode = valueNode->nextValueNode;
                        }
                    }

                    if (erasedValues == keyNode->childCount)
                    {
                        KeyNodePtr toBeDestroyed = keyNode;

                        if (!previousKeyNode)
                        {
                            m_KeyNodePtrArray[keyHash] = keyNode->nextKeyNode;
                        }
                        else
                        {
                            previousKeyNode->nextKeyNode = keyNode->nextKeyNode;
                        }
                        
                        m_KeyNodeAllocator.destroy(toBeDestroyed);
                        m_KeyNodeAllocator.deallocate_one(toBeDestroyed);
                    }
                    else
                    {
                        keyNode->childCount -= erasedValues;
                    }
                }
            }

            (*m_Size) -= erasedValues;
            return erasedValues;
        }
        else
        {
            return 0ul;
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
        const size_t keyHash = m_KeyHasher(key) % m_BucketCount;
        
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

public:
    static constexpr size_t DEFAULT_FILE_SISE {1024 * 1024 * 1024ul};
    static constexpr size_t DEFAULT_BUCKET_COUNT {2 * size_t(1e6)};

private:
    const size_t m_BucketCount;

    size_t* m_Size;
    bipc::managed_mapped_file m_MappedFile;
    KeyAllocator m_KeyAllocator;
    ValueAllocator m_ValueAllocator;
    KeyNodeAllocator m_KeyNodeAllocator;
    ValueNodeAllocator m_ValueNodeAllocator;
    KeyHash m_KeyHasher;
    KeyNodePtr* m_KeyNodePtrArray;
};
} //HardDriveContainers
