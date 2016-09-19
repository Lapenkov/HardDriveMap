#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/functional/hash.hpp>
#include <string>
#include <functional>
#include <memory>

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
private:
    using ValuePtr = typename ValueAllocator::pointer;
    using KeyPtr = typename KeyAllocator::pointer;

    struct ValueNodeT;
    using ValueNodeAllocator = bipc::allocator<ValueNodeT, bipc::managed_mapped_file::segment_manager>;
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
    using KeyNodeAllocator = bipc::allocator<KeyNodeT, bipc::managed_mapped_file::segment_manager>;
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

public:
    using key_type = Key;
    using value_type = Value;

    Map(const char* filename, const size_t fileSize = DEFAULT_FILE_SIZE, const size_t bucketCount = DEFAULT_BUCKET_COUNT)
        : m_BucketCount(bucketCount)
        , m_Filename(filename)
        , m_MappedFile(new bipc::managed_mapped_file(boost::interprocess::open_or_create, filename, fileSize))
        , m_KeyAllocator(m_MappedFile->get_segment_manager())
        , m_ValueAllocator(m_MappedFile->get_segment_manager())
        , m_KeyNodeAllocator(m_MappedFile->get_segment_manager())
        , m_ValueNodeAllocator(m_MappedFile->get_segment_manager())
    {
        const size_t minFileSize = m_BucketCount * sizeof(KeyNodePtr) + sizeof(size_t) + MAX_PAIR_SIZE * 10;
        if (fileSize < minFileSize)
        {
            m_MappedFile->grow(filename, minFileSize - fileSize);
            RemapFile();
        }

        m_KeyNodePtrArray = m_MappedFile->find_or_construct<KeyNodePtr>("KeyNodePtrArray")[m_BucketCount](nullptr);
        m_Size = m_MappedFile->find_or_construct<size_t>("Size")(0);
    }

    Map(const Map&) = delete;
    Map& operator =(const Map&) = delete;

    Map(Map&& rhv)
        : m_BucketCount(rhv.m_BucketCount)
        , m_Filename(std::move(rhv.m_Filename))
        , m_Size(rhv.m_Size)
        , m_MappedFile(std::move(rhv.m_MappedFile))
        , m_KeyAllocator(std::move(rhv.m_KeyAllocator))
        , m_ValueAllocator(std::move(rhv.m_ValueAllocator))
        , m_KeyNodeAllocator(std::move(rhv.m_KeyNodeAllocator))
        , m_ValueNodeAllocator(std::move(rhv.m_ValueNodeAllocator))
        , m_KeyHasher(std::move(rhv.m_KeyHasher))
        , m_KeyNodePtrArray(rhv.m_KeyNodePtrArray)
    {}

    Map& operator=(Map&& rhv)
    {
        m_BucketCount = rhv.m_BucketCount;
        m_Filename = std::move(rhv.m_Filename);
        m_Size = rhv.m_Size;
        m_MappedFile = std::move(rhv.m_MappedFile);
        m_KeyAllocator = std::move(rhv.m_KeyAllocator);
        m_ValueAllocator = std::move(rhv.m_ValueAllocator);
        m_KeyNodeAllocator = std::move(rhv.m_KeyNodeAllocator);
        m_ValueNodeAllocator = std::move(rhv.m_ValueNodeAllocator);
        m_KeyHasher = std::move(rhv.m_KeyHasher);
        m_KeyNodePtrArray = rhv.m_KeyNodePtrArray;
    }

    template <typename ProvidedKeyT, typename ProvidedValueT>
    void Insert(const ProvidedKeyT& key, const ProvidedValueT& value)
    {
        if (GetSegmentManager()->get_free_memory() < MAX_PAIR_SIZE)
        {
            bipc::managed_mapped_file::grow(m_Filename.c_str(), GetSegmentManager()->get_size() / 2);
            RemapFile();
        }
        InsertImpl(ConstructParam<Key, ProvidedKeyT>(key), ConstructParam<Value, ProvidedValueT>(value));
    }

    template <typename ProvidedKeyT>
    ValuePtr Find(const ProvidedKeyT& key)
    {
        ValueNodePtr valueNode = FindImpl(ConstructParam<Key, ProvidedKeyT>(key));

        return (valueNode) ? valueNode->storedValue : nullptr;
    }

    template <typename ProvidedKeyT>
    ValueNodePtr FindAll(const ProvidedKeyT& key)
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
        return m_MappedFile->get_segment_manager();
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

    ValueNodePtr FindImpl(Key&& key)
    {
        std::pair<KeyNodePtr*, bool> result = FindKeyNode(key);
        bool foundKey = result.second;
        KeyNodePtr* keyNode = result.first;

        return (foundKey) ? (*keyNode)->valueNode : nullptr;
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

                        m_ValueAllocator.destroy(toBeDestroyed->storedValue);
                        m_ValueAllocator.deallocate_one(toBeDestroyed->storedValue);

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

                    m_KeyAllocator.destroy(toBeDestroyed->storedKey);
                    m_KeyAllocator.deallocate_one(toBeDestroyed->storedKey);

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

                            m_ValueAllocator.destroy(toBeDestroyed->storedValue);
                            m_ValueAllocator.deallocate_one(toBeDestroyed->storedValue);

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
                        
                        m_KeyAllocator.destroy(toBeDestroyed->storedKey);
                        m_KeyAllocator.deallocate_one(toBeDestroyed->storedKey);

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

    void RemapFile()
    {
        m_MappedFile.reset(new bipc::managed_mapped_file(bipc::open_only, m_Filename.c_str()));
        KeyAllocator newKeyAlloc(m_MappedFile->get_segment_manager());
        ValueAllocator newValueAlloc(m_MappedFile->get_segment_manager());
        KeyNodeAllocator newKeyNodeAlloc(m_MappedFile->get_segment_manager());
        ValueNodeAllocator newValueNodeAlloc(m_MappedFile->get_segment_manager());

        swap(newKeyAlloc, m_KeyAllocator);
        swap(newValueAlloc, m_ValueAllocator);
        swap(newKeyNodeAlloc, m_KeyNodeAllocator);
        swap(newValueNodeAlloc, m_ValueNodeAllocator);

        m_KeyNodePtrArray = m_MappedFile->find<KeyNodePtr>("KeyNodePtrArray").first;
        m_Size = m_MappedFile->find<size_t>("Size").first;
    }

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
    static constexpr size_t DEFAULT_FILE_SIZE {128 * 1024 * 1024ul};
    static constexpr size_t DEFAULT_BUCKET_COUNT {2 * size_t(1e6)};
    static constexpr size_t MAX_PAIR_SIZE {2 * 256 * 1024 + sizeof(KeyNodeT) + sizeof(ValueNodeT)};

private:
    const size_t m_BucketCount;
    const std::string m_Filename;

    size_t* m_Size;
    std::unique_ptr<bipc::managed_mapped_file> m_MappedFile;
    KeyAllocator m_KeyAllocator;
    ValueAllocator m_ValueAllocator;
    KeyNodeAllocator m_KeyNodeAllocator;
    ValueNodeAllocator m_ValueNodeAllocator;
    KeyHash m_KeyHasher;
    KeyNodePtr* m_KeyNodePtrArray;
};
} //HardDriveContainers
