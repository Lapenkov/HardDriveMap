#include "container.h"
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/log/trivial.hpp>

int main(int argc, const char** argv)
{
    if (argc < 3)
    {
        BOOST_LOG_TRIVIAL(error) << "Please, provide 2 command line args like [scan|find] [<path>|<filename>]";
        return 1;
    }
    const char* storageFile = "storage.bin";
    namespace bipc = boost::interprocess;

    using CharAllocator = bipc::allocator<char, bipc::managed_mapped_file::segment_manager>;
    using string = bipc::basic_string<char, std::char_traits<char>, CharAllocator>;

    namespace fs = boost::filesystem;

    if (std::string(argv[1]) == "scan")
    {
        std::remove(storageFile);
        HardDriveContainers::Map<string, string> filePathStorage(storageFile);

        fs::path folder(argv[2]);

        if (fs::is_directory(folder))
        {
            fs::recursive_directory_iterator dir(folder), end;

            BOOST_LOG_TRIVIAL(info) << "Started scanning folder '" << folder.string() << "'";
            size_t processedFiles = 0;

            for (; dir != end; )
            {
                const std::string pathStr = dir->path().string();

                try
                {
                    if (fs::is_regular_file(dir->path()))
                    {
                        const std::string fileName = dir->path().filename().string();

                        filePathStorage.Insert(fileName.c_str(), pathStr.c_str());
                        ++processedFiles;

                        if (processedFiles % 10000 == 0)
                        {
                            BOOST_LOG_TRIVIAL(info) << "Scanned " << processedFiles << " files";
                        }
                    }
                    
                    ++dir;
                }
                catch (const fs::filesystem_error& ex)
                {
                    BOOST_LOG_TRIVIAL(error) << "Filesystem error while processing " << pathStr << ": " << ex.what();
                    dir.no_push();
                    ++dir;
                }
                catch (const bipc::bad_alloc& ex)
                {
                    BOOST_LOG_TRIVIAL(error) << "Error allocating memory while processing " << pathStr << ": "  << ex.what();
                }
            }
            
            BOOST_LOG_TRIVIAL(info) << "Finished scanning: " << processedFiles << " files scanned";
        }
        else
        {
            BOOST_LOG_TRIVIAL(error) << "Second argument should be a directory";
        }
    }
    else if (std::string(argv[1]) == "find")
    {
        HardDriveContainers::Map<string, string> filePathStorage(storageFile);
        auto valueNodePtr = filePathStorage.FindAll(argv[2]);

        if (valueNodePtr == nullptr)
        {
            BOOST_LOG_TRIVIAL(info) << "No path found";
        }
        else
        {
            for (; valueNodePtr; valueNodePtr = valueNodePtr->nextValueNode)
            {
                BOOST_LOG_TRIVIAL(info) << *valueNodePtr->storedValue;
            }
        }
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << "First argument should be 'scan' or 'find'";
    }

    return 0;
}
